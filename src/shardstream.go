package shardstream

import (
    "bufio"
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "net"
)

type PeerOptions struct {
    ListenPort int
    CoordinatorHost string
}

type HandshakeInfo struct {
    peerListeningOn int
}

func RunCoordinator(port int) {
    portStr := fmt.Sprintf("%d", port)
    fmt.Println("Listening on port: " + portStr)
    listener, err := net.Listen("tcp", ":" + portStr)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    for {
        if conn, err := listener.Accept(); err != nil {
            log.Println(err)
        } else {
            go handleConnection(conn)
        }
    }
}

func handleConnection(conn net.Conn) {
    defer conn.Close()

    if info, err := receiveHandshake(conn); err != nil {
        log.Println(err)
    } else {
        fmt.Printf("Peer is listening on: %d", info.peerListeningOn)

        if _, err := conn.Write([]byte("Stream page 1\n")); err != nil {
            log.Println("Failed to stream page 1")
            log.Println(err)
        }
    }
}

func receiveHandshake(conn net.Conn) (*HandshakeInfo, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }

    info := &HandshakeInfo { int(binary.BigEndian.Uint64(currentWord)) }
    return info, nil
}

func sendHandshake(conn net.Conn, info HandshakeInfo) (error) {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(info.peerListeningOn))
    _, err := conn.Write(currentWord)
    return err
}

func RunPeer(options PeerOptions) {
    conn, err := net.Dial("tcp", options.CoordinatorHost)
    if err != nil {
        log.Fatal(err)
    }

    if err := sendHandshake(conn, HandshakeInfo { options.ListenPort } ); err != nil {
        log.Fatal(err)
    }

    page, err := bufio.NewReader(conn).ReadString('\n')
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(page)
}
