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
package io.trino.plugin.redis;

import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.plugin.redis.RedisClientManager.parseClusterMasterNodes;
import static org.assertj.core.api.Assertions.assertThat;

final class TestRedisClusterNodeDiscovery
{
    @Test
    void testDiscoversAllHealthyMastersAcrossShards()
    {
        // Realistic CLUSTER NODES output for a 3-master, 3-replica cluster.
        String clusterNodes = """
                07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
                67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 5461-10922
                292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 10923-16383
                6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
                824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
                e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460
                """;

        assertThat(parseClusterMasterNodes(clusterNodes)).containsExactlyInAnyOrder(
                HostAddress.fromParts("127.0.0.1", 30001),
                HostAddress.fromParts("127.0.0.1", 30002),
                HostAddress.fromParts("127.0.0.1", 30003));
    }

    @Test
    void testExcludesFailedMasters()
    {
        String clusterNodes = """
                aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 127.0.0.1:30001@31001 master - 0 1426238316232 1 connected 0-8191
                bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 127.0.0.1:30002@31002 master,fail - 0 1426238317741 2 disconnected 8192-16383
                """;

        assertThat(parseClusterMasterNodes(clusterNodes)).containsExactly(
                HostAddress.fromParts("127.0.0.1", 30001));
    }

    @Test
    void testIgnoresBlankAndMalformedLines()
    {
        String clusterNodes = """

                cccccccccccccccccccccccccccccccccccccccc 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-16383

                incomplete-line
                """;

        assertThat(parseClusterMasterNodes(clusterNodes)).containsExactly(
                HostAddress.fromParts("127.0.0.1", 30001));
    }

    @Test
    void testReturnsEmptyWhenNoMasters()
    {
        String clusterNodes = """
                dddddddddddddddddddddddddddddddddddddddd 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
                """;

        Set<HostAddress> masters = parseClusterMasterNodes(clusterNodes);
        assertThat(masters).isEmpty();
    }
}
