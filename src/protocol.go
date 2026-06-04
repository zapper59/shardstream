package shardstream

import (
    "bufio"
    "encoding/binary"
    "io"
    "net"
    "strings"
)

// Handshake metadata indicating which halves of the data stream are involved.
type ShardData uint64
const AB ShardData = 3 // Indicates a request for a full data stream. Ie. a non-sharded data source.

type HandshakeInfo struct {
    shards ShardData
    peerListeningOn string
}

type HandshakeAck struct {
    redirectTo map[ShardData]HandshakeInfo // Empty when no redirect is required.
}

func sendHandshake(conn io.Writer, info HandshakeInfo) (error) {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(info.shards))
    _, err := conn.Write(currentWord)
    if err != nil {
        return err
    }

    _, err = conn.Write([]byte(info.peerListeningOn))
    if err != nil {
        return err
    }
    _, err = conn.Write([]byte{0})
    return err
}

func receiveHandshake(conn net.Conn) (*HandshakeInfo, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }
    shards := binary.BigEndian.Uint64(currentWord)

    peerListeningOn, err := bufio.NewReader(conn).ReadString(0)
    if err != nil {
        return nil, err
    }
    peerListeningOn = strings.TrimRight(peerListeningOn, "\x00")
    info := &HandshakeInfo { ShardData(shards), peerListeningOn }
    return info, nil
}

func sendHandshakeAck(conn io.Writer, ack HandshakeAck) (error) {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(len(ack.redirectTo)))
    _, err := conn.Write(currentWord)
    if err != nil {
        return err
    }

    for _, info := range ack.redirectTo {
        if err := sendHandshake(conn, info); err != nil {
            return err
        }
    }

    return nil
}

func receiveHandshakeAck(conn net.Conn) (*HandshakeAck, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }
    redirectToLen := binary.BigEndian.Uint64(currentWord)

    ack := HandshakeAck { make(map[ShardData]HandshakeInfo) }
    for i := 0; uint64(i) < redirectToLen; i++ {
        info, err := receiveHandshake(conn)
        if err != nil {
            return nil, err
        }

        ack.redirectTo[info.shards] = *info
    }

    return &ack, nil
}

