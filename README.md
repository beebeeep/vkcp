VKCP - a better Sentinel for Valkey
===================================

VKCP (Valkey Controller and Proxy) is a transparent proxy for Valkey database that doubles as an external HA controller, performing automatic valkey master failovers.

## How it works
VKCP nodes coordinate to select a leader that will periodically query status of all configured Valkey servers and distribute this information among followers.
When some client connects to any VKCP node (leader or follower), it will proxy its connection to the master of Valkey cluster. This happens transparently,
i.e. you don't have to configure your Valkey client to work in sentinel mode.

When leader detects that master has failed, it will select a new master among remaining healthy replicas (the one with maximum replication offset) and reconfigure
all servers to replicate from them. When old master will come back online, it also will be reconfigured as replica. 

In case some network segment with VKCP nodes and Valkey servers becomes isolated (from the quorum of remaining nodes), isolated proxies will stop proxying the
traffic in order to limit the scale of data inconsistency between valkey servers.
