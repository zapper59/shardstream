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

const ReadBufferSize = 1024

func RunCoordinator(streamSource io.Reader, port int) {
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
            go handleConnection(streamSource, conn)
        }
    }
}

func pipeData(streamSource io.Reader, streamOutput io.Writer) {
    readBuffer := make([]byte, ReadBufferSize)
    for {
        bytesRead, err := streamSource.Read(readBuffer)
        if err != nil {
            log.Println(err)
            return
        }

        if _, err := streamOutput.Write(readBuffer[:bytesRead]); err != nil {
            log.Println(err)
            return
        }
    }
}

func handleConnection(streamSource io.Reader, conn net.Conn) {
    defer conn.Close()

    if info, err := receiveHandshake(conn); err != nil {
        log.Println(err)
    } else {
        fmt.Printf("Peer is listening on: %d\n", info.peerListeningOn)
        pipeData(streamSource, conn)
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

func RunPeer(streamOutput io.Writer, options PeerOptions) {
    conn, err := net.Dial("tcp", options.CoordinatorHost)
    if err != nil {
        log.Fatal(err)
    }

    if err := sendHandshake(conn, HandshakeInfo { options.ListenPort } ); err != nil {
        log.Fatal(err)
    }

    pipeData(bufio.NewReader(conn), streamOutput)
}
