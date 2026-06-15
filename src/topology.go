package shardstream

import (
    "io"
    "log"
    "log/slog"
    "net"
)

type RedirectAllowed bool

type RemotePeer struct {
    shards ShardData
    canRedirect RedirectAllowed
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

func (self *RemotePeerTable) countChildren() uint64 {
    return uint64(len(self.remotePeers))
}

func (self *RemotePeerTable) countGrandchildren() uint64 {
    grandchildCount := uint64(0)

    for _, peer := range self.remotePeers {
        grandchildCount += peer.childPeers
    }

    return grandchildCount
}

func (self *RemotePeerTable) incrementChildPeers(uid uint64) {
    tempPeer := self.remotePeers[uid]
    tempPeer.childPeers += 1
    self.remotePeers[uid] = tempPeer
}

func (self *RemotePeerTable) computeRedirectLocked(
    shards ShardData,
) RedirectTable {
    redirectTo := make(map[ShardData]ListenAddress)

    minChildPeers := MaxUint64
    optimalPeerAddress := ListenAddress("invalid_hostname")
    optimalPeerUID := MaxUint64
    for uid, peer := range self.remotePeers {
        if peer.childPeers < minChildPeers && peer.canRedirect {
            minChildPeers = peer.childPeers
            optimalPeerAddress = peer.listeningOn
            optimalPeerUID = uid
        }
    }
    self.incrementChildPeers(optimalPeerUID)

    redirectTo[shards] = optimalPeerAddress
    return RedirectTable { redirectTo }
}

func (self *RemotePeerTable) computeMultiShardRedirectLocked(
    requestedShards ShardData,
) RedirectTable {
    redirectTo := make(map[ShardData]ListenAddress)
    shard := FirstShard

    for uid, peer := range self.remotePeers {
        redirectTo[shard] = peer.listeningOn
        self.incrementChildPeers(uid)
        shard = shard.nextShard(self.shardsInStream)
    }

    return RedirectTable { redirectTo }
}

func (self *RemotePeerTable) connectPeerLocked(
    addr ListenAddress, shards ShardData, canRedirect RedirectAllowed,
) uint64 {
    self.peerUIDAllocator += 1
    connectedUid := self.peerUIDAllocator

    self.remotePeers[connectedUid] = RemotePeer {
        shards,
        canRedirect,
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
    childCount := self.countChildren()
    grandchildCount := self.countGrandchildren()
    requestedShards := everyShard(self.shardsInStream) & info.requestedShards
    requestedShardCount := requestedShards.countShards()

    slog.Debug(
        "Computing Topology",
        "bandwidth",
        remainingBandwidth,
        "grandchildCount",
        grandchildCount,
        "requestedShards",
        requestedShards,
        "requestedShardCount",
        requestedShardCount,
    )

    if remainingBandwidth >= requestedShardCount {
        nowServing = requestedShards
        connectedUid = self.connectPeerLocked(
            info.peerListeningOn, nowServing, RedirectAllowed(true),
        )
    } else if childCount == uint64(requestedShardCount) && grandchildCount == 0 {
        redirectTo = self.computeMultiShardRedirectLocked(requestedShards)
    } else if remainingBandwidth == 1 && grandchildCount == 0 {
        nowServing = FirstShard
        redirect := requestedShards - nowServing
        redirectTo = self.computeRedirectLocked(redirect)
        connectedUid = self.connectPeerLocked(
            info.peerListeningOn, nowServing, RedirectAllowed(false),
        )
    } else {
        redirectTo = self.computeRedirectLocked(requestedShards)
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

        for shards, addr := range ack.redirectTo.addressByShard {
            info2 := Handshake{ shards, info.peerListeningOn }
            discovery = combineDiscoveryTables(
                discovery, runDiscovery(info2, addr),
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
