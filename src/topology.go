package shardstream

import (
    "log"
    "net"
)

type RemotePeer struct {
    UID uint64
    childPeers uint64
    listeningOn ListenAddress
}

type RemotePeerTable struct {
    shardsInStream ShardCount
    shardsToServe ShardCount
    peerUIDAllocator uint64
    remotePeers map[uint64]RemotePeer
}

func newRemotePeerTable(shardsInStream ShardCount) (RemotePeerTable) {
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

func (self *RemotePeerTable) computeRedirectLocked() (HandshakeAck) {
    ack := HandshakeAck { self.shardsInStream, make(map[ShardData]ListenAddress) }

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

    ack.redirectTo[everyShard(self.shardsInStream)] = optimalPeerAddress
    return ack
}

func (self *RemotePeerTable) connectPeerLocked(addr ListenAddress) (uint64) {
    self.peerUIDAllocator += 1
    connectedUid := self.peerUIDAllocator

    self.remotePeers[connectedUid] = RemotePeer {
        0, // Start with no childPeers.
        connectedUid,
        addr,
    }

    return connectedUid
}

func (self *RemotePeerTable) redirectPeerOrConnectLocked(
    info Handshake,
) (uint64, HandshakeAck) {
    connectedUid := MaxUint64
    ack := HandshakeAck { self.shardsInStream, make(map[ShardData]ListenAddress) }

    remoteShards := len(self.remotePeers)
    if uint64(remoteShards) >= uint64(self.shardsToServe) {
        ack = self.computeRedirectLocked()
    } else {
        connectedUid = self.connectPeerLocked(info.peerListeningOn)
    }

    return connectedUid, ack
}

func runDiscovery(info Handshake, host string) (net.Conn, ShardCount){
    conn, err := net.Dial("tcp", host)
    if err != nil {
        log.Fatal(err)
    }

    if err := sendHandshake(conn, info); err != nil {
        log.Fatal(err)
    }

    ack, err := receiveHandshakeAck(conn)
    if err != nil {
        log.Fatal(err)
    }

    if len(ack.redirectTo) == 0 {
        return conn, ack.shards
    } else {
        conn.Close()
        redirectShardData := everyShard(ack.shards)
        return runDiscovery(info, string(ack.redirectTo[redirectShardData]))
    }
}
