package shardstream

import (
    "github.com/zapper59/abstractGoNet"
    "io"
    "log"
    "log/slog"
)

type redirectAllowed bool

type remotePeer struct {
    shards shardData
    canRedirect redirectAllowed
    childPeers uint64
    listeningOn ListenAddress
}

type remotePeerTable struct {
    shardsInStream ShardCount
    shardsToServe ShardCount
    peerUIDAllocator uint64
    remotePeers map[uint64]remotePeer
}

func newRemotePeerTable(shardsInStream ShardCount) remotePeerTable {
    return remotePeerTable {
        shardsInStream,
        shardsInStream + 1,
        0,
        make(map[uint64]remotePeer),
    }
}

func (self *remotePeerTable) dropPeerLocked(uid uint64) {
    delete(self.remotePeers, uid)
}

func (self *remotePeerTable) countRemainingBandwidth() ShardCount {
    remainingBandwidth := self.shardsToServe

    for _, peer := range self.remotePeers {
        remainingBandwidth -= peer.shards.countShards()
    }

    return remainingBandwidth
}

func (self *remotePeerTable) countChildren() uint64 {
    return uint64(len(self.remotePeers))
}

func (self *remotePeerTable) countGrandchildren() uint64 {
    grandchildCount := uint64(0)

    for _, peer := range self.remotePeers {
        grandchildCount += peer.childPeers
    }

    return grandchildCount
}

func (self *remotePeerTable) incrementChildPeers(uid uint64) {
    tempPeer := self.remotePeers[uid]
    tempPeer.childPeers += 1
    self.remotePeers[uid] = tempPeer
}

func (self *remotePeerTable) computeRedirectLocked(
    shards shardData,
) redirectTable {
    redirectTo := make(map[shardData]ListenAddress)

    minChildPeers := maxUint64
    optimalPeerAddress := ListenAddress("invalid_hostname")
    optimalPeerUID := maxUint64
    for uid, peer := range self.remotePeers {
        if peer.childPeers < minChildPeers && peer.canRedirect {
            minChildPeers = peer.childPeers
            optimalPeerAddress = peer.listeningOn
            optimalPeerUID = uid
        }
    }
    self.incrementChildPeers(optimalPeerUID)

    redirectTo[shards] = optimalPeerAddress
    return redirectTable { redirectTo }
}

func (self *remotePeerTable) computeMultiShardRedirectLocked(
    requestedShards shardData,
) redirectTable {
    redirectTo := make(map[shardData]ListenAddress)
    shard := firstShard

    for uid, peer := range self.remotePeers {
        redirectTo[shard] = peer.listeningOn
        self.incrementChildPeers(uid)
        shard = shard.nextShard(self.shardsInStream)
    }

    return redirectTable { redirectTo }
}

func (self *remotePeerTable) connectPeerLocked(
    addr ListenAddress, shards shardData, canRedirect redirectAllowed,
) uint64 {
    self.peerUIDAllocator += 1
    connectedUid := self.peerUIDAllocator

    self.remotePeers[connectedUid] = remotePeer {
        shards,
        canRedirect,
        0, // Start with no childPeers.
        addr,
    }

    return connectedUid
}

func (self *remotePeerTable) redirectPeerOrConnectLocked(
    info handshake,
) (uint64, redirectTable, shardData) {
    connectedUid := maxUint64
    redirectTo := redirectTable { make(map[shardData]ListenAddress) }
    nowServing := noShards

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
            info.peerListeningOn, nowServing, redirectAllowed(true),
        )
    } else if childCount == uint64(requestedShardCount) && grandchildCount == 0 {
        redirectTo = self.computeMultiShardRedirectLocked(requestedShards)
    } else if remainingBandwidth == 1 && grandchildCount == 0 {
        nowServing = firstShard
        redirect := requestedShards - nowServing
        redirectTo = self.computeRedirectLocked(redirect)
        connectedUid = self.connectPeerLocked(
            info.peerListeningOn, nowServing, redirectAllowed(false),
        )
    } else {
        redirectTo = self.computeRedirectLocked(requestedShards)
    }

    return connectedUid, redirectTo, nowServing
}

type discoveryTable struct {
    shards ShardCount
    parents map[shardData]io.Reader
    shardIndices shardIndices
}

func combineDiscoveryTables(
    a discoveryTable, b discoveryTable,
) discoveryTable {
    discovery := discoveryTable {
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

func runDiscovery(
    info handshake, hostname ListenAddress, host abstractGoNet.Net,
) discoveryTable {
    slog.Debug("Dialing", "host", hostname)
    conn, err := host.Dial("tcp", string(hostname))
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

        discovery := discoveryTable {
            ack.shards,
            make(map[shardData]io.Reader),
            ack.nowServing,
        }

        for shards, addr := range ack.redirectTo.addressByShard {
            info2 := handshake{ shards, info.peerListeningOn }
            discovery = combineDiscoveryTables(
                discovery, runDiscovery(info2, addr, host),
            )
        }

        return discovery
    } else {
        parents := make(map[shardData]io.Reader)

        if len(ack.redirectTo.addressByShard) == 0 {
            all := everyShard(ack.shards) & info.requestedShards
            parents[all] = conn
        } else {
            for shard, _ := range ack.nowServing.lastByteByShard {
                parents[shard] = conn
            }
        }

        discovery := discoveryTable {
            ack.shards,
            parents,
            ack.nowServing,
        }

        for shard, addr := range ack.redirectTo.addressByShard {
            info2 := handshake{ shard, info.peerListeningOn }
            discovery = combineDiscoveryTables(
                discovery, runDiscovery(info2, addr, host),
            )
        }

        return discovery
    }
}
