package shardstream

type ConnectedPeer struct {
    UID uint64
    streamOutput PageWriter
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

func (self *Multiplexer) sendDataLocked(data PageData) {
    for _, peer := range self.connectedPeers {
        if err := peer.streamOutput.SendPageData(data); err != nil {
            peer.errorLog <- err
        }
    }
}

func (self *Multiplexer) registerConnectionLocked(
    connectedUid uint64, streamOutput PageWriter, errorLog chan error,
) {
    self.connectedPeers[connectedUid] = ConnectedPeer { connectedUid, streamOutput, errorLog }
}
