package main

import (
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "net"
    "os"
)

func main() {
    port := os.Args[1]
    fmt.Println("Listening on port: " + port)
    runCoordinator(port)
}

type HandshakeInfo struct {
    peerListeningOn int
}

func runCoordinator(port string) {
    listener, err := net.Listen("tcp", ":" + port)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    for {
        if conn, err := listener.Accept(); err != nil {
            log.Println(err)
        } else {
            if info, err := receiveHandshake(conn); err != nil {
                log.Println(err)
            } else {
                fmt.Printf("%d", info.peerListeningOn)
            }
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

