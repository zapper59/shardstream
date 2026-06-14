package shardstream

import (
    "iter"
    "log/slog"
)

type SingleShardStream struct {
    lastByte uint64 
    next func() (*PageData, error, bool)
}

func newTwoShardRecombinator(
    a iter.Seq2[*PageData, error],
    aLastByte uint64,
    b iter.Seq2[*PageData, error],
    bLastByte uint64,
) iter.Seq2[*PageData, error] {
    shardCount := ShardCount(2)
    aShard := FirstShard
    bShard := aShard.nextShard(shardCount)
    incomingStreams := make(map[ShardData]SingleShardStream)

    aNext, _ := iter.Pull2(a)
    incomingStreams[aShard] = SingleShardStream {
        aLastByte, aNext,
    }
    
    bNext, _ := iter.Pull2(b)
    incomingStreams[bShard] = SingleShardStream {
        bLastByte, bNext,
    }

    var nextShardToRead ShardData
    if bLastByte < aLastByte {
        nextShardToRead = bShard
    } else {
        nextShardToRead = aShard
    }

    return func(yield func(*PageData, error) bool) {
        for {
            slog.Debug("recieving", "shard", nextShardToRead)
            page, err, ok := incomingStreams[nextShardToRead].next()
            if !ok {
                return
            }
            if err != nil {
                yield(nil, err)
                return
            }
            slog.Debug("recv", "shard", nextShardToRead)

            tempSingleShard := incomingStreams[nextShardToRead]
            incomingStreams[nextShardToRead] = SingleShardStream {
                page.startingByte + uint64(page.length),
                tempSingleShard.next,
            }
            nextNextShard := nextShardToRead.nextShard(shardCount)
            targetStartByte := incomingStreams[nextNextShard].lastByte

            if incomingStreams[nextShardToRead].lastByte > targetStartByte {
                nextShardToRead = nextNextShard

                if !yield(page, nil) {
                    return
                }
            }
        }
    }
}
