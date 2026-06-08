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

func everyShard(count ShardCount) ShardData {
    return ShardData((1 << count) - 1)
}

type HandshakeInfo struct {
    peerListeningOn string
}

type HandshakeAck struct {
    shards ShardCount
    redirectTo map[ShardData]HandshakeInfo // Empty when no redirect is required.
}

const MaxUint16 = ^uint16(0)
const ReadBufferSize = MaxUint16

type PageData struct {
    length uint16
    data [MaxUint16]byte
}

func sendHandshake(conn io.Writer, info HandshakeInfo) error {
    _, err := conn.Write([]byte(info.peerListeningOn))
    if err != nil {
        return err
    }
    _, err = conn.Write([]byte{0})
    return err
}

func receiveHandshake(conn io.Reader) (*HandshakeInfo, error) {
    peerListeningOn, err := bufio.NewReader(conn).ReadString(0)
    if err != nil {
        return nil, err
    }
    peerListeningOn = strings.TrimRight(peerListeningOn, "\x00")
    info := &HandshakeInfo { peerListeningOn }
    return info, nil
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

    for shardData, info := range ack.redirectTo {
        binary.BigEndian.PutUint64(currentWord, uint64(shardData))
        _, err = conn.Write(currentWord)
        if err != nil {
            return err
        }

        if err := sendHandshake(conn, info); err != nil {
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

    ack := HandshakeAck { ShardCount(shards), make(map[ShardData]HandshakeInfo) }
    for i := 0; uint64(i) < redirectToLen; i++ {
        if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
            return nil, err
        }
        shardData := binary.BigEndian.Uint64(currentWord)

        info, err := receiveHandshake(conn)
        if err != nil {
            return nil, err
        }

        ack.redirectTo[ShardData(shardData)] = *info
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
        for {
            page := pagePool.Get().(*PageData)

            bytesRead, err := conn.Read(page.data[:])
            if err != nil {
                yield(nil, err)
                return
            }

            page.length = uint16(bytesRead)
            if !yield(page, err) {
                return
            }
        }
    }
}

// Consume an encoded series of pages producing a series of PageData.
func newPageReader(conn io.Reader) iter.Seq2[*PageData, error] {
    return func(yield func(*PageData, error) bool) {
        for {
            lengthBytes := make([]byte, 2)
            if _, err := io.ReadAtLeast(conn, lengthBytes, 2); err != nil {
                yield(nil, err)
                return
            }
            page := pagePool.Get().(*PageData)
            page.length = binary.BigEndian.Uint16(lengthBytes)

            _, err := io.ReadAtLeast(conn, page.data[:page.length], int(page.length))
            if err != nil {
                yield(nil, err)
                return
            }

            if !yield(page, err) {
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
    currentWord := make([]byte, 2)
    binary.BigEndian.PutUint16(currentWord, page.length)
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
