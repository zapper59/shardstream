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

func (self *RemotePeerTable) computeRedirectLocked() RedirectTable {
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

    redirectTo[everyShard(self.shardsInStream)] = optimalPeerAddress
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

    if remainingBandwidth >= self.shardsInStream {
        nowServing = everyShard(self.shardsInStream)
        connectedUid = self.connectPeerLocked(info.peerListeningOn, nowServing)
    } else {
        redirectTo = self.computeRedirectLocked()
    }

    return connectedUid, redirectTo, nowServing
}

type DiscoveryTable struct {
    shards ShardCount
    parents map[ShardData]io.Reader
    shardIndices ShardIndices
}

func runDiscovery(info Handshake, host string) DiscoveryTable {
    slog.Debug("Dialing", "host", host)
    conn, err := net.Dial("tcp", host)
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

        redirectShardData := everyShard(ack.shards)
        return runDiscovery(
            info,
            string(ack.redirectTo.addressByShard[redirectShardData]),
        )
    } else {
        parents := make(map[ShardData]io.Reader)
        parents[everyShard(ack.shards)] = conn
        return DiscoveryTable {
            ack.shards,
            parents,
            ack.nowServing,
        }
    }
}
