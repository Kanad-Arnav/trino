/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.redis.RedisClusterTopology;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkState;

/**
 * Manages a real multi-primary Redis Cluster using a single Docker container
 * running multiple Redis instances on different ports.
 * <p>
 * Starts {@code numPrimaries} Redis instances with cluster mode enabled within
 * one container, forms them into a cluster by distributing hash slots evenly,
 * and provides seed addresses and a {@link RedisClusterClient} for cluster-aware
 * data loading.
 * <p>
 * Using a single container avoids cross-container networking issues: all Redis
 * instances share the same network namespace and can reach each other via
 * {@code 127.0.0.1}. Fixed port bindings (7000, 7001, ...) make the advertised
 * addresses reachable from the test JVM as well.
 */
public class RedisCluster
        implements Closeable
{
    private static final int DEFAULT_NUM_PRIMARIES = 3;
    private static final int DEFAULT_BASE_PORT = 7000;
    private static final int CLUSTER_TIMEOUT_MILLIS = 5000;

    // RedisCluster uses fixed host port bindings, so only one cluster can be active at a time.
    private static final Semaphore CLUSTER_SEMAPHORE = new Semaphore(1);

    private final GenericContainer<?> container;
    private final List<RedisClient> clients;
    private final List<HostAndPort> jedisSeedAddresses;
    private final List<com.google.common.net.HostAndPort> seedAddresses;
    private final RedisClusterClient redisClusterClient;

    public RedisCluster()
    {
        this(DEFAULT_NUM_PRIMARIES, DEFAULT_BASE_PORT);
    }

    public RedisCluster(int numPrimaries, int basePort)
    {
        clients = new ArrayList<>(numPrimaries);
        jedisSeedAddresses = new ArrayList<>(numPrimaries);
        seedAddresses = new ArrayList<>(numPrimaries);

        // RedisCluster binds fixed host ports; serialize so concurrent tests do not conflict.
        CLUSTER_SEMAPHORE.acquireUninterruptibly();
        boolean acquired = true;

        try {
            startCluster(numPrimaries, basePort);
            acquired = false;
        }
        finally {
            if (acquired) {
                CLUSTER_SEMAPHORE.release();
            }
        }
    }

    private void startCluster(int numPrimaries, int basePort)
    {
        // Build the command to start all Redis instances in a single container.
        // Each instance gets its own data directory to avoid AOF/cluster-config file conflicts.
        List<Integer> ports = new ArrayList<>(numPrimaries);
        for (int i = 0; i < numPrimaries; i++) {
            ports.add(basePort + i);
        }

        StringBuilder command = new StringBuilder();
        command.append("mkdir -p");
        for (int i = 0; i < numPrimaries; i++) {
            command.append(" /data/").append(i);
        }
        command.append("; ");

        for (int i = 0; i < numPrimaries; i++) {
            int port = ports.get(i);
            command.append("redis-server")
                    .append(" --port ").append(port)
                    .append(" --cluster-enabled yes")
                    .append(" --cluster-config-file nodes.conf")
                    .append(" --cluster-node-timeout ").append(CLUSTER_TIMEOUT_MILLIS)
                    .append(" --dir /data/").append(i)
                    .append(" --appendonly no");
            command.append(" & ");
        }
        command.append("wait");

        // Expose all ports with fixed bindings so cluster-announce-port is reachable from the test JVM
        ImmutableList.Builder<String> portBindings = ImmutableList.builder();
        for (int port : ports) {
            portBindings.add(port + ":" + port);
        }

        container = new GenericContainer<>("redis:" + RedisServer.LATEST_VERSION)
                .withExposedPorts(ports.toArray(new Integer[0]))
                .withCommand("/bin/sh", "-c", command.toString());
        container.setPortBindings(portBindings.build());
        container.start();

        // Create clients for each Redis instance and set cluster-announce-ip/port
        String announceIp = "127.0.0.1";
        for (int i = 0; i < numPrimaries; i++) {
            int port = ports.get(i);
            RedisClient client = RedisClient.builder()
                    .hostAndPort(announceIp, port)
                    .clientConfig(DefaultJedisClientConfig.builder().build())
                    .build();
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-ip", announceIp);
                connection.getStatusCodeReply();
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-port", Integer.toString(port));
                connection.getStatusCodeReply();
            }
            clients.add(client);
            jedisSeedAddresses.add(new HostAndPort(announceIp, port));
            seedAddresses.add(com.google.common.net.HostAndPort.fromParts(announceIp, port));
        }

        // Form the cluster: MEET all nodes, then distribute slots
        formCluster(ports);

        // Create RedisClusterClient for cluster-aware data loading
        redisClusterClient = RedisClusterClient.create(ImmutableSet.copyOf(jedisSeedAddresses));
    }

    private void formCluster(List<Integer> ports)
    {
        int numPrimaries = ports.size();

        // Meet all nodes from the first node
        RedisClient firstClient = clients.get(0);
        for (int i = 1; i < numPrimaries; i++) {
            try (Connection connection = firstClient.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "MEET", "127.0.0.1", Integer.toString(ports.get(i)));
                connection.getStatusCodeReply();
            }
        }

        // Distribute slots evenly across primaries
        int slotsPerNode = RedisClusterTopology.TOTAL_SLOTS / numPrimaries;
        int remainder = RedisClusterTopology.TOTAL_SLOTS % numPrimaries;
        int currentSlot = 0;
        for (int i = 0; i < numPrimaries; i++) {
            int slotsForThisNode = slotsPerNode + (i < remainder ? 1 : 0);
            if (slotsForThisNode > 0) {
                int endSlot = currentSlot + slotsForThisNode - 1;
                try (Connection connection = clients.get(i).getPool().getResource()) {
                    connection.sendCommand(
                            Protocol.Command.CLUSTER,
                            "ADDSLOTSRANGE",
                            Integer.toString(currentSlot),
                            Integer.toString(endSlot));
                    connection.getStatusCodeReply();
                }
                currentSlot = endSlot + 1;
            }
        }

        // Wait for cluster to be ready
        waitForClusterReady(firstClient);

        // Verify all slots are assigned
        verifyClusterState();
    }

    private void waitForClusterReady(RedisClient client)
    {
        long deadlineMillis = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadlineMillis) {
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "INFO");
                String info = SafeEncoder.encode((byte[]) connection.getOne());
                if (info != null && info.contains("cluster_state:ok")) {
                    return;
                }
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Redis cluster to be ready", e);
            }
        }
        throw new IllegalStateException("Redis cluster did not become ready within 60 seconds");
    }

    private void verifyClusterState()
    {
        try (Connection connection = clients.get(0).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SLOTS");
            Object response = connection.getOne();
            checkState(response instanceof List, "CLUSTER SLOTS returned unexpected type: %s", response.getClass());
            @SuppressWarnings("unchecked")
            List<Object> slotRanges = (List<Object>) response;
            int assignedSlots = 0;
            for (Object slotRangeObj : slotRanges) {
                @SuppressWarnings("unchecked")
                List<Object> slotRange = (List<Object>) slotRangeObj;
                int startSlot = ((Long) slotRange.get(0)).intValue();
                int endSlot = ((Long) slotRange.get(1)).intValue();
                assignedSlots += (endSlot - startSlot + 1);
            }
            checkState(assignedSlots == RedisClusterTopology.TOTAL_SLOTS,
                    "Cluster has %d assigned slots, expected %d",
                    assignedSlots,
                    RedisClusterTopology.TOTAL_SLOTS);
        }
    }

    /**
     * Returns the seed addresses for the cluster, suitable for use as {@code redis.nodes}.
     */
    public List<com.google.common.net.HostAndPort> getSeedAddresses()
    {
        return ImmutableList.copyOf(seedAddresses);
    }

    /**
     * Returns a comma-separated list of seed addresses for the {@code redis.nodes} property.
     */
    public String getSeedAddressesString()
    {
        return seedAddresses.stream()
                .map(com.google.common.net.HostAndPort::toString)
                .reduce((a, b) -> a + "," + b)
                .orElseThrow();
    }

    /**
     * Returns a {@link RedisClusterClient} for cluster-aware data loading.
     */
    public RedisClusterClient getRedisClusterClient()
    {
        return redisClusterClient;
    }

    /**
     * Returns a {@link RedisClient} connected to the first primary, for direct operations.
     */
    public RedisClient getClient()
    {
        return clients.get(0);
    }

    @Override
    public void close()
    {
        try {
            redisClusterClient.close();
        }
        catch (Exception e) {
            // ignore
        }
        for (RedisClient client : clients) {
            try {
                client.close();
            }
            catch (Exception e) {
                // ignore
            }
        }
        try {
            container.close();
        }
        catch (Exception e) {
            // ignore
        }
        finally {
            CLUSTER_SEMAPHORE.release();
        }
    }
}
