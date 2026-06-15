package shardstream

import (
    "io"
    "log"
    "log/slog"
    "net"
)

type RemotePeer struct {
    shards ShardData
    childPeers uint64
    listeningOn ListenAddress
}

type RemotePeerTable struct {
    shardsInStream ShardCount
    shardsToServe ShardCount
    peerUIDAllocator uint64
    remotePeers map[uint64]RemotePeer
}

func newRemotePeerTable(shardsInStream ShardCount) RemotePeerTable {
    return RemotePeerTable {
        shardsInStream,
        shardsInStream + 1,
        0,
        make(map[uint64]RemotePeer),
    }
}

func (self *RemotePeerTable) dropPeerLocked(uid uint64) {
    delete(self.remotePeers, uid)
}

func (self *RemotePeerTable) countRemainingBandwidth() ShardCount {
    remainingBandwidth := self.shardsToServe

    for _, peer := range self.remotePeers {
        remainingBandwidth -= peer.shards.countShards()
    }

    return remainingBandwidth
}

func (self *RemotePeerTable) countGrandchildren() uint64 {
    grandchildren := uint64(0)

    for _, peer := range self.remotePeers {
        grandchildren += peer.childPeers
    }

    return grandchildren
}

func (self *RemotePeerTable) computeRedirectLocked(
    shards ShardData,
) RedirectTable {
    redirectTo := make(map[ShardData]ListenAddress)

    minChildPeers := MaxUint64
    optimalPeerAddress := ListenAddress("invalid_hostname")
    optimalPeerUID := MaxUint64
    for uid, peer := range self.remotePeers {
        if peer.childPeers < minChildPeers {
            minChildPeers = peer.childPeers
            optimalPeerAddress = peer.listeningOn
            optimalPeerUID = uid
        }
    }
    tempPeer := self.remotePeers[optimalPeerUID]
    tempPeer.childPeers += 1
    self.remotePeers[optimalPeerUID] = tempPeer

    redirectTo[shards] = optimalPeerAddress
    return RedirectTable { redirectTo }
}

func (self *RemotePeerTable) connectPeerLocked(
    addr ListenAddress, shards ShardData,
) uint64 {
    self.peerUIDAllocator += 1
    connectedUid := self.peerUIDAllocator

    self.remotePeers[connectedUid] = RemotePeer {
        shards,
        0, // Start with no childPeers.
        addr,
    }

    return connectedUid
}

func (self *RemotePeerTable) redirectPeerOrConnectLocked(
    info Handshake,
) (uint64, RedirectTable, ShardData) {
    connectedUid := MaxUint64
    redirectTo := RedirectTable { make(map[ShardData]ListenAddress) }
    nowServing := NoShards

    remainingBandwidth := self.countRemainingBandwidth()
    grandchildren := self.countGrandchildren()
    requestedShards := everyShard(self.shardsInStream) & info.requestedShards
    requestedShardCount := requestedShards.countShards()

    slog.Debug(
        "Computing Topology",
        "bandwidth",
        remainingBandwidth,
        "grandchildren",
        grandchildren,
        "requestedShards",
        requestedShards,
        "requestedShardCount",
        requestedShardCount,
    )

    if remainingBandwidth >= requestedShardCount {
        nowServing = requestedShards
        connectedUid = self.connectPeerLocked(info.peerListeningOn, nowServing)
    } else if remainingBandwidth == 1 && grandchildren == 0 {
        nowServing = FirstShard
        redirect := requestedShards - nowServing
        redirectTo = self.computeRedirectLocked(redirect)
        connectedUid = self.connectPeerLocked(info.peerListeningOn, nowServing)
    } else {
        redirect := requestedShards
        redirectTo = self.computeRedirectLocked(redirect)
    }

    return connectedUid, redirectTo, nowServing
}

type DiscoveryTable struct {
    shards ShardCount
    parents map[ShardData]io.Reader
    shardIndices ShardIndices
}

func combineDiscoveryTables(
    a DiscoveryTable, b DiscoveryTable,
) DiscoveryTable {
    discovery := DiscoveryTable {
        a.shards,
        a.parents,
        a.shardIndices,
    }
    for k, v := range b.parents {
        discovery.parents[k] = v
    }
    for k, v := range b.shardIndices.lastByteByShard {
        discovery.shardIndices.lastByteByShard[k] = v
    }
    return discovery
}

func runDiscovery(info Handshake, host ListenAddress) DiscoveryTable {
    slog.Debug("Dialing", "host", host)
    conn, err := net.Dial("tcp", string(host))
    if err != nil {
        log.Fatal(err)
    }

    slog.Debug("Begin Handshake")
    if err := sendHandshake(conn, info); err != nil {
        log.Fatal(err)
    }

    ack, err := receiveHandshakeAck(conn)
    if err != nil {
        log.Fatal(err)
    }
    slog.Debug(
        "Ack Received",
        "redirect",
        ack.redirectTo.addressByShard,
        "nowServing",
        ack.nowServing.lastByteByShard,
    )

    if len(ack.nowServing.lastByteByShard) == 0 {
        conn.Close()

        discovery := DiscoveryTable {
            ack.shards,
            make(map[ShardData]io.Reader),
            ack.nowServing,
        }

        for _, addr := range ack.redirectTo.addressByShard {
            discovery = combineDiscoveryTables(
                discovery, runDiscovery(info, addr),
            )
        }

        return discovery
    } else {
        parents := make(map[ShardData]io.Reader)

        if len(ack.redirectTo.addressByShard) == 0 {
            all := everyShard(ack.shards) & info.requestedShards
            parents[all] = conn
        } else {
            for shard, _ := range ack.nowServing.lastByteByShard {
                parents[shard] = conn
            }
        }

        discovery := DiscoveryTable {
            ack.shards,
            parents,
            ack.nowServing,
        }

        for shard, addr := range ack.redirectTo.addressByShard {
            info2 := Handshake{ shard, info.peerListeningOn }
            discovery = combineDiscoveryTables(
                discovery, runDiscovery(info2, addr),
            )
        }

        return discovery
    }
}
