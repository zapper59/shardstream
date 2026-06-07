package shardstream

import (
    "io"
)

const MaxUint16 = ^uint16(0)
const ReadBufferSize = MaxUint16

type ConnectedPeer struct {
    UID uint64
    streamOutput io.Writer
    errorLog chan error
}

type Multiplexer struct {
    connectedPeers map[uint64]ConnectedPeer
}

func newMultiplexer() (Multiplexer) {
    return Multiplexer {
        connectedPeers: make(map[uint64]ConnectedPeer),
    }
}

func (self *Multiplexer) dropPeerLocked(uid uint64) {
    delete(self.connectedPeers, uid)
}

func (self *Multiplexer) sendDataLocked(data []byte) {
    for _, peer := range self.connectedPeers {
        if _, err := peer.streamOutput.Write(data); err != nil {
            peer.errorLog <- err
        }
    }
}

func (self *Multiplexer) registerConnectionLocked(
    connectedUid uint64, streamOutput io.Writer, errorLog chan error,
) {
        self.connectedPeers[connectedUid] = ConnectedPeer { connectedUid, streamOutput, errorLog }
}
