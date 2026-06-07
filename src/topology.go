package shardstream

import (
    "errors"
    "io"
)

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

func (self *RemotePeerTable) computeRedirectLocked() (HandshakeAck, string) {
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
    return ack, optimalPeerAddress
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

func (self *RemotePeerTable) redirectOrConnectPeerLocked(
    info *HandshakeInfo, streamOutput io.Writer, peerErrorLog chan error,
) (uint64, HandshakeAck) {
    connectedUid := MaxUint64

    remotePeers := len(self.remotePeers)
    ack := HandshakeAck { make(map[ShardData]HandshakeInfo) }

    if info != nil && remotePeers >= BranchingFactor {
        redirectAck, optimalPeerAddress := self.computeRedirectLocked()
        ack = redirectAck
        peerErrorLog <- errors.New("Redirect to: " + optimalPeerAddress)
    } else {
        connectedUid = self.connectPeerLocked(info)
    }

    return connectedUid, ack
}
