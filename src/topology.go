package shardstream

import (
    "log"
    "net"
)

const BranchingFactor = 2 // The number of non-local peers to allow before sending redirects.

type RemotePeer struct {
    UID uint64
    childPeers uint64
    info HandshakeInfo
}

type RemotePeerTable struct {
    peerUIDAllocator uint64
    remotePeers map[uint64]RemotePeer
}

func newRemotePeerTable() (RemotePeerTable) {
    return RemotePeerTable {0, make(map[uint64]RemotePeer)}
}

func (self *RemotePeerTable) dropPeerLocked(uid uint64) {
    delete(self.remotePeers, uid)
}

func (self *RemotePeerTable) computeRedirectLocked() (HandshakeAck) {
    ack := HandshakeAck { make(map[ShardData]HandshakeInfo) }

    minChildPeers := MaxUint64
    optimalPeerAddress := "invalid_hostname"
    optimalPeerUID := MaxUint64
    for uid, peer := range self.remotePeers {
        if peer.childPeers < minChildPeers {
            minChildPeers = peer.childPeers
            optimalPeerAddress = peer.info.peerListeningOn
            optimalPeerUID = uid
        }
    }
    tempPeer := self.remotePeers[optimalPeerUID]
    tempPeer.childPeers += 1
    self.remotePeers[optimalPeerUID] = tempPeer

    ack.redirectTo[AB] = HandshakeInfo { AB, optimalPeerAddress }
    return ack
}

func (self *RemotePeerTable) connectPeerLocked(info *HandshakeInfo) (uint64) {
    self.peerUIDAllocator += 1
    connectedUid := self.peerUIDAllocator

    if info != nil {
        self.remotePeers[connectedUid] = RemotePeer {
            0, // Start with no childPeers.
            connectedUid,
            *info,
        }
    }

    return connectedUid
}

func (self *RemotePeerTable) redirectPeerOrConnectLocked(
    info *HandshakeInfo,
) (uint64, HandshakeAck) {
    connectedUid := MaxUint64
    ack := HandshakeAck { make(map[ShardData]HandshakeInfo) }

    remotePeers := len(self.remotePeers)
    if info != nil && remotePeers >= BranchingFactor {
        ack = self.computeRedirectLocked()
    } else {
        connectedUid = self.connectPeerLocked(info)
    }

    return connectedUid, ack
}

func runDiscovery(info HandshakeInfo, host string) (net.Conn){
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
        return conn
    } else {
        conn.Close()
        return runDiscovery(info, ack.redirectTo[AB].peerListeningOn)
    }
}
