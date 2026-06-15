package shardstream

import (
    "iter"
    "log/slog"
)

type singleShardStream struct {
    lastByte uint64 
    next func() (*pageData, error, bool)
}

func newTwoShardRecombinator(
    a iter.Seq2[*pageData, error],
    aLastByte uint64,
    b iter.Seq2[*pageData, error],
    bLastByte uint64,
) iter.Seq2[*pageData, error] {
    shardCount := ShardCount(2)
    aShard := firstShard
    bShard := aShard.nextShard(shardCount)
    incomingStreams := make(map[shardData]singleShardStream)

    aNext, _ := iter.Pull2(a)
    incomingStreams[aShard] = singleShardStream {
        aLastByte, aNext,
    }

    bNext, _ := iter.Pull2(b)
    incomingStreams[bShard] = singleShardStream {
        bLastByte, bNext,
    }

    var nextShardToRead shardData
    if bLastByte < aLastByte {
        nextShardToRead = bShard
    } else {
        nextShardToRead = aShard
    }

    return func(yield func(*pageData, error) bool) {
        for {
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
            incomingStreams[nextShardToRead] = singleShardStream {
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
