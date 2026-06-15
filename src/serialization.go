package shardstream

import (
    "encoding/binary"
    "io"
    "iter"
    "math/bits"
    "sync"
)

// Handshake metadata indicating how many slices the data is divided into.
type ShardCount uint64

// Handshake metadata indicating which slices of the data stream are to be included.
type shardData uint64
const initiallyRequestedShardData shardData = shardData(maxUint64)
const firstShard shardData = shardData(1)
const noShards shardData = shardData(0)

func everyShard(count ShardCount) shardData {
    return shardData((1 << count) - 1)
}

func (shard shardData) nextShard(count ShardCount) shardData{
    next := uint64(shard) << 1
    if next >= (1 << count) {
        return firstShard
    }

    return shardData(next)
}

func (shard shardData) countShards() ShardCount {
    return ShardCount(bits.OnesCount64(uint64(shard)))
}

// A TCP listen address in the form of that accepted by [net.Listen].
type ListenAddress string

type handshake struct {
    requestedShards shardData
    peerListeningOn ListenAddress
}

type redirectTable struct {
    addressByShard map[shardData]ListenAddress
}

type shardIndices struct {
    lastByteByShard map[shardData]uint64
}

type handshakeAck struct {
    shards ShardCount
    redirectTo redirectTable
    nowServing shardIndices
}

const maxUint64 = ^uint64(0)
const maxUint16 = ^uint16(0)
const readBufferSize = maxUint16

type pageData struct {
    startingByte uint64
    length uint16
    data [readBufferSize]byte
}

func sendListenAddress(conn io.Writer, addr ListenAddress) error {
    currentWord := make([]byte, 2)
    binary.BigEndian.PutUint16(currentWord, uint16(len(addr)))
    _, err := conn.Write(currentWord)
    if err != nil {
        return err
    }

    _, err = conn.Write([]byte(addr))
    if err != nil {
        return err
    }
    return err
}

func receiveListenAddress(conn io.Reader) (*ListenAddress, error) {
    currentWord := make([]byte, 2)
    if _, err := io.ReadFull(conn, currentWord); err != nil {
        return nil, err
    }
    addrLen := binary.BigEndian.Uint16(currentWord)

    page := pagePool.Get().(*pageData)
    if _, err := io.ReadFull(conn, page.data[:addrLen]); err != nil {
        return nil, err
    }
    peerListeningOn := string(page.data[:addrLen])
    addr := ListenAddress(peerListeningOn)
    return &addr, nil
}

func sendHandshake(conn io.Writer, info handshake) error {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(
        currentWord, uint64(info.requestedShards),
    )
    _, err := conn.Write(currentWord)
    if err != nil {
        return err
    }

    return sendListenAddress(conn, info.peerListeningOn)
}

func receiveHandshake(conn io.Reader) (*handshake, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadFull(conn, currentWord); err != nil {
        return nil, err
    }
    requestedShards := shardData(binary.BigEndian.Uint64(currentWord))

    addr, err := receiveListenAddress(conn)
    if err != nil {
        return nil, err
    }

    return &handshake{ requestedShards, *addr }, nil
}

func sendHandshakeAck(conn io.Writer, ack handshakeAck) error {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(ack.shards))
    _, err := conn.Write(currentWord)
    if err != nil {
        return err
    }

    binary.BigEndian.PutUint64(
        currentWord,
        uint64(len(ack.redirectTo.addressByShard)),
    )
    _, err = conn.Write(currentWord)
    if err != nil {
        return err
    }

    for shardData, addr := range ack.redirectTo.addressByShard {
        binary.BigEndian.PutUint64(currentWord, uint64(shardData))
        _, err = conn.Write(currentWord)
        if err != nil {
            return err
        }

        if err := sendListenAddress(conn, addr); err != nil {
            return err
        }
    }

    binary.BigEndian.PutUint64(
        currentWord,
        uint64(len(ack.nowServing.lastByteByShard)),
    )
    _, err = conn.Write(currentWord)
    if err != nil {
        return err
    }

    for shardData, lastByte := range ack.nowServing.lastByteByShard {
        binary.BigEndian.PutUint64(currentWord, uint64(shardData))
        _, err = conn.Write(currentWord)
        if err != nil {
            return err
        }

        binary.BigEndian.PutUint64(currentWord, uint64(lastByte))
        _, err = conn.Write(currentWord)
        if err != nil {
            return err
        }
    }

    return nil
}

func receiveHandshakeAck(conn io.Reader) (*handshakeAck, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadFull(conn, currentWord); err != nil {
        return nil, err
    }
    shards := binary.BigEndian.Uint64(currentWord)

    if _, err := io.ReadFull(conn, currentWord); err != nil {
        return nil, err
    }
    redirectToLen := binary.BigEndian.Uint64(currentWord)

    redirectTo := make(map[shardData]ListenAddress)
    for i := 0; uint64(i) < redirectToLen; i++ {
        if _, err := io.ReadFull(conn, currentWord); err != nil {
            return nil, err
        }
        shards := binary.BigEndian.Uint64(currentWord)

        addr, err := receiveListenAddress(conn)
        if err != nil {
            return nil, err
        }

        redirectTo[shardData(shards)] = *addr
    }

    if _, err := io.ReadFull(conn, currentWord); err != nil {
        return nil, err
    }
    nowServingLen := binary.BigEndian.Uint64(currentWord)

    nowServing:= make(map[shardData]uint64)
    for i := 0; uint64(i) < nowServingLen; i++ {
        if _, err := io.ReadFull(conn, currentWord); err != nil {
            return nil, err
        }
        shards := binary.BigEndian.Uint64(currentWord)

        if _, err := io.ReadFull(conn, currentWord); err != nil {
            return nil, err
        }
        lastByte := binary.BigEndian.Uint64(currentWord)

        nowServing[shardData(shards)] = lastByte
    }

    ack := handshakeAck {
        ShardCount(shards),
        redirectTable { redirectTo },
        shardIndices { nowServing },
    }
    return &ack, nil
}

var pagePool = sync.Pool{
    New: func() any {
        return new(pageData)
    },
}

// Consume a raw sequence of bytes, producing a series of pageData.
func newPaginator(conn io.Reader) iter.Seq2[*pageData, error] {
    return func(yield func(*pageData, error) bool) {
        var startingByte uint64 = 0
        for {
            page := pagePool.Get().(*pageData)
            page.startingByte = startingByte

            bytesRead, err := conn.Read(page.data[:])
            if err != nil {
                yield(nil, err)
                return
            }

            startingByte += (1 + uint64(bytesRead))
            page.length = uint16(bytesRead)
            if !yield(page, nil) {
                return
            }
        }
    }
}

// Consume an encoded series of pages producing a series of pageData.
func newPageReader(conn io.Reader) iter.Seq2[*pageData, error] {
    return func(yield func(*pageData, error) bool) {
        for {
            pageHeader := make([]byte, 10)
            if _, err := io.ReadFull(conn, pageHeader); err != nil {
                yield(nil, err)
                return
            }
            page := pagePool.Get().(*pageData)
            page.startingByte = binary.BigEndian.Uint64(pageHeader[:8])
            page.length = binary.BigEndian.Uint16(pageHeader[8:])

            _, err := io.ReadFull(conn, page.data[:page.length])
            if err != nil {
                yield(nil, err)
                return
            }

            if !yield(page, nil) {
                return
            }
        }
    }
}

type pageWriter interface {
    sendPageData(pageData) error
}

// Encode a sequence of pageData onto a connection to be read via newPageReader.
type pageSerializer struct {
    w io.Writer
}

func newPageSerializer(w io.Writer) pageSerializer {
    return pageSerializer { w }
}

func (self *pageSerializer) sendPageData(page pageData) error {
    currentWord := make([]byte, 10)
    binary.BigEndian.PutUint64(currentWord[:8], page.startingByte)
    binary.BigEndian.PutUint16(currentWord[8:], page.length)
    if _, err := self.w.Write(currentWord); err != nil {
        return err
    }

    if _, err := self.w.Write(page.data[:page.length]); err != nil {
        return err
    }

    return nil
}

// Encode the raw sequence of bytes that is represented by a sequence of pageData.
type depaginator struct {
    w io.Writer
}

func newDepaginator(w io.Writer) depaginator {
    return depaginator { w }
}

func (self *depaginator) sendPageData(page pageData) error {
    _, err := self.w.Write(page.data[:page.length])
    return err
}
