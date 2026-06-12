package shardstream

import (
    "bufio"
    "encoding/binary"
    "io"
    "iter"
    "strings"
    "sync"
)

// Handshake metadata indicating how many slices the data is divided into.
type ShardCount uint64

// Handshake metadata indicating which slices of the data stream are to be included.
type ShardData uint64
const InitiallyRequestedShardData ShardData = ShardData(MaxUint64)
const FirstShard ShardData = ShardData(1)

func everyShard(count ShardCount) ShardData {
    return ShardData((1 << count) - 1)
}

func (shard ShardData) nextShard(count ShardCount) ShardData{
    next := uint64(shard) << 1
    if next >= (1 << count) {
        return FirstShard
    }

    return ShardData(next)
}

type ListenAddress string

type Handshake struct {
    peerListeningOn ListenAddress
}

type HandshakeAck struct {
    shards ShardCount
    redirectTo map[ShardData]ListenAddress // Empty when no redirect is required.
}

const MaxUint16 = ^uint16(0)
const ReadBufferSize = MaxUint16

type PageData struct {
    startingByte uint64
    length uint16
    data [MaxUint16]byte
}

func sendListenAddress(conn io.Writer, addr ListenAddress) error {
    _, err := conn.Write([]byte(addr))
    if err != nil {
        return err
    }
    _, err = conn.Write([]byte{0})
    return err
}

func receiveListenAddress(conn io.Reader) (*ListenAddress, error) {
    peerListeningOn, err := bufio.NewReader(conn).ReadString(0)
    if err != nil {
        return nil, err
    }
    peerListeningOn = strings.TrimRight(peerListeningOn, "\x00")
    addr := ListenAddress(peerListeningOn)
    return &addr, nil
}

func sendHandshake(conn io.Writer, info Handshake) error {
    return sendListenAddress(conn, info.peerListeningOn)
}

func receiveHandshake(conn io.Reader) (*Handshake, error) {
    addr, err := receiveListenAddress(conn)
    if err != nil {
        return nil, err
    }

    return &Handshake{ *addr }, nil
}

func sendHandshakeAck(conn io.Writer, ack HandshakeAck) error {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(ack.shards))
    _, err := conn.Write(currentWord)
    if err != nil {
        return err
    }

    binary.BigEndian.PutUint64(currentWord, uint64(len(ack.redirectTo)))
    _, err = conn.Write(currentWord)
    if err != nil {
        return err
    }

    for shardData, addr := range ack.redirectTo {
        binary.BigEndian.PutUint64(currentWord, uint64(shardData))
        _, err = conn.Write(currentWord)
        if err != nil {
            return err
        }

        if err := sendListenAddress(conn, addr); err != nil {
            return err
        }
    }

    return nil
}

func receiveHandshakeAck(conn io.Reader) (*HandshakeAck, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }
    shards := binary.BigEndian.Uint64(currentWord)

    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }
    redirectToLen := binary.BigEndian.Uint64(currentWord)

    ack := HandshakeAck { ShardCount(shards), make(map[ShardData]ListenAddress) }
    for i := 0; uint64(i) < redirectToLen; i++ {
        if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
            return nil, err
        }
        shardData := binary.BigEndian.Uint64(currentWord)

        addr, err := receiveListenAddress(conn)
        if err != nil {
            return nil, err
        }

        ack.redirectTo[ShardData(shardData)] = *addr
    }

    return &ack, nil
}

var pagePool = sync.Pool{
    New: func() any {
        return new(PageData)
    },
}

// Consume a raw sequence of bytes, producing a series of PageData.
func newPaginator(conn io.Reader) iter.Seq2[*PageData, error] {
    return func(yield func(*PageData, error) bool) {
        var startingByte uint64 = 0
        for {
            page := pagePool.Get().(*PageData)
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

// Consume an encoded series of pages producing a series of PageData.
func newPageReader(conn io.Reader) iter.Seq2[*PageData, error] {
    return func(yield func(*PageData, error) bool) {
        for {
            pageHeader := make([]byte, 10)
            if _, err := io.ReadAtLeast(conn, pageHeader, 10); err != nil {
                yield(nil, err)
                return
            }
            page := pagePool.Get().(*PageData)
            page.startingByte = binary.BigEndian.Uint64(pageHeader[:8])
            page.length = binary.BigEndian.Uint16(pageHeader[8:])

            _, err := io.ReadAtLeast(conn, page.data[:page.length], int(page.length))
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

type PageWriter interface {
    SendPageData(PageData) error
}

// Encode a sequence of PageData onto a connection to be read via newPageReader.
type PageSerializer struct {
    w io.Writer
}

func newPageSerializer(w io.Writer) PageSerializer {
    return PageSerializer { w }
}

func (self *PageSerializer) SendPageData(page PageData) error {
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

// Encode the raw sequence of bytes that is represented by a sequence of PageData.
type Depaginator struct {
    w io.Writer
}

func newDepaginator(w io.Writer) Depaginator {
    return Depaginator { w }
}

func (self *Depaginator) SendPageData(page PageData) error {
    _, err := self.w.Write(page.data[:page.length])
    return err
}
