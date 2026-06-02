# shardstream

## Introduction

The shardstream protocol is Bittorrent for livestreams. It defines a general purpose framework for streaming data through a tree shaped network built of one coordinator node and many peer nodes.

## TODO

1. Shard data streams across nodes without creating cycles. As a peer I would like to consume all shards of the data stream while serving only one slice of the stream to downstream peers.
2. "loss resistance": As a peer if my upstream node disconnects I would like to be able to reconnect to the network seamlessly without losing data.
3. RTMP livestreaming module
4. Ping-based handshake. As a coordinator I would like the edges of my network to be as short as possible.
5. "auto-rebalancing": When a large branch of the network fails, the protocol should rebuild the network with knowledge of the depth of the resulting orphaned networks, maintaining minimal network
   depth.
