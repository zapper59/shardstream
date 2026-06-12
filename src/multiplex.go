package shardstream

type ConnectedPeer struct {
    UID uint64
    streamOutput PageWriter
    errorLog chan error
}

type Multiplexer struct {
    connectedPeers map[uint64]ConnectedPeer
    shardIndices ShardIndices
}

func newMultiplexer(shardIndices ShardIndices) (Multiplexer) {
    return Multiplexer {
        make(map[uint64]ConnectedPeer),
        shardIndices,
    }
}

func (self *Multiplexer) dropPeerLocked(uid uint64) {
    delete(self.connectedPeers, uid)
}

func (self *Multiplexer) sendDataLocked(data PageData) {
    self.shardIndices.lastByteByShard[FirstShard] =
        data.startingByte + uint64(data.length)

    for _, peer := range self.connectedPeers {
        if err := peer.streamOutput.SendPageData(data); err != nil {
            peer.errorLog <- err
        }
    }
}

func (self *Multiplexer) registerConnectionLocked(
    shards ShardData,
    connectedUid uint64,
    streamOutput PageWriter,
    errorLog chan error,
) ShardIndices {
    self.connectedPeers[connectedUid] = ConnectedPeer { connectedUid, streamOutput, errorLog }

    return self.shardIndices
}
