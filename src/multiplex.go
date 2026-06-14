package shardstream

type ConnectedPeer struct {
    UID uint64
    servingShards ShardData
    streamOutput PageWriter
    errorLog chan error
}

type Multiplexer struct {
    connectedPeers map[uint64]ConnectedPeer
    shardsInStream ShardCount
    lastWrittenShard ShardData
    shardIndices ShardIndices
}

func newMultiplexer(shardsInStream ShardCount, shardIndices ShardIndices) (Multiplexer) {
    bestLastByte := uint64(0)
    bestShard := FirstShard
    for shard, lastByte := range shardIndices.lastByteByShard {
        if lastByte >= bestLastByte {
            bestLastByte = lastByte
            bestShard = shard
        }
    }

    return Multiplexer {
        make(map[uint64]ConnectedPeer),
        shardsInStream,
        bestShard,
        shardIndices,
    }
}

func (self *Multiplexer) dropPeerLocked(uid uint64) {
    delete(self.connectedPeers, uid)
}

func (self *Multiplexer) sendDataLocked(data PageData) {
    thisShard := self.lastWrittenShard.nextShard(self.shardsInStream)
    self.shardIndices.lastByteByShard[thisShard] =
        data.startingByte + uint64(data.length)
    self.lastWrittenShard = thisShard

    for _, peer := range self.connectedPeers {
        if thisShard & peer.servingShards != 0 {
            if err := peer.streamOutput.SendPageData(data); err != nil {
                peer.errorLog <- err
            }
        }
    }
}

func (self *Multiplexer) registerConnectionLocked(
    servingShards ShardData,
    connectedUid uint64,
    streamOutput PageWriter,
    errorLog chan error,
) ShardIndices {
    self.connectedPeers[connectedUid] = ConnectedPeer {
        connectedUid, servingShards, streamOutput, errorLog,
    }

    toReturn := ShardIndices{ make(map[ShardData]uint64) }
    for shard, lastByte := range self.shardIndices.lastByteByShard {
        if shard & servingShards != 0 {
            toReturn.lastByteByShard[shard] = lastByte
        }
    }
    return toReturn
}
