package shardstream

type connectedPeer struct {
    UID uint64
    servingShards shardData
    streamOutput pageWriter
    errorLog chan error
}

type multiplexer struct {
    connectedPeers map[uint64]connectedPeer
    shardsInStream ShardCount
    lastWrittenShard shardData
    shardIndices shardIndices
}

func newMultiplexer(shardsInStream ShardCount, shardIndices shardIndices) (multiplexer) {
    bestLastByte := uint64(0)
    bestShard := firstShard

    currShard := firstShard
    for _ = range shardsInStream {
        lastByte := shardIndices.lastByteByShard[currShard]
        if lastByte >= bestLastByte {
            bestLastByte = lastByte
            bestShard = currShard
        }
        currShard = currShard.nextShard(shardsInStream)
    }

    return multiplexer {
        make(map[uint64]connectedPeer),
        shardsInStream,
        bestShard,
        shardIndices,
    }
}

func (self *multiplexer) dropPeerLocked(uid uint64) {
    delete(self.connectedPeers, uid)
}

func (self *multiplexer) sendDataLocked(data pageData) {
    thisShard := self.lastWrittenShard.nextShard(self.shardsInStream)
    self.shardIndices.lastByteByShard[thisShard] =
        data.startingByte + uint64(data.length)
    self.lastWrittenShard = thisShard

    for _, peer := range self.connectedPeers {
        if thisShard & peer.servingShards != 0 {
            if err := peer.streamOutput.sendPageData(data); err != nil {
                peer.errorLog <- err
            }
        }
    }
}

func (self *multiplexer) registerConnectionLocked(
    servingShards shardData,
    connectedUid uint64,
    streamOutput pageWriter,
    errorLog chan error,
) shardIndices {
    self.connectedPeers[connectedUid] = connectedPeer {
        connectedUid, servingShards, streamOutput, errorLog,
    }

    toReturn := shardIndices{ make(map[shardData]uint64) }
    for shard, lastByte := range self.shardIndices.lastByteByShard {
        if shard & servingShards != 0 {
            toReturn.lastByteByShard[shard] = lastByte
        }
    }
    return toReturn
}
