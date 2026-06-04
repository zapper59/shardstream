package shardstream

import (
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "net"
    "sync"
)

type PeerOptions struct {
    ListenPort int
    CoordinatorHost string
}

type HandshakeInfo struct {
    peerListeningOn int
}

const ReadBufferSize = 1024 * 1024

func RunCoordinator(streamSource io.Reader, port int) {
    portStr := fmt.Sprintf("%d", port)
    fmt.Println("Listening on port: " + portStr)
    listener, err := net.Listen("tcp", ":" + portStr)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    multiplexer := newMultiplexer()
    go multiplexer.driveDataStream(streamSource)

    for {
        if conn, err := listener.Accept(); err != nil {
            log.Println(err)
        } else {
            go handleConnection(&multiplexer, streamSource, conn)
        }
    }
}

type ConnectedPeer struct {
    UID uint64
    info HandshakeInfo
    streamOutput io.Writer
    errorLog chan error
}

type Multiplexer struct {
    mutex sync.Mutex

    peerUIDAllocator uint64
    connectedPeers map[uint64]ConnectedPeer
}

func newMultiplexer() (Multiplexer) {
    return Multiplexer {
        peerUIDAllocator: 0,
        connectedPeers: make(map[uint64]ConnectedPeer),
    }
}

func (self *Multiplexer) driveDataStream(streamSource io.Reader) {
    readBuffer := make([]byte, ReadBufferSize)
    for {
        bytesRead, err := streamSource.Read(readBuffer)
        if err != nil {
            log.Println(err)
            return
        }

        self.mutex.Lock()

        for _, peer := range self.connectedPeers {
            if _, err := peer.streamOutput.Write(readBuffer[:bytesRead]); err != nil {
                peer.errorLog <- err
            }
        }

        self.mutex.Unlock()
    }
}

func (self *Multiplexer) dropPeer(uid uint64) {
    self.mutex.Lock()
    delete(self.connectedPeers, uid)
    self.mutex.Unlock()
}

func (self *Multiplexer) registerPeerAndWaitForError(info HandshakeInfo, streamOutput io.Writer) (error) {
    self.mutex.Lock()

    peer := ConnectedPeer {
        self.peerUIDAllocator,
        info,
        streamOutput,
        make(chan error),
    }

    self.peerUIDAllocator += 1
    self.connectedPeers[peer.UID] = peer

    self.mutex.Unlock()

    defer self.dropPeer(peer.UID)
    return <-peer.errorLog
}

func handleConnection(multiplexer *Multiplexer, streamSource io.Reader, conn net.Conn) {
    defer conn.Close()

    if info, err := receiveHandshake(conn); err != nil {
        log.Println(err)
    } else {
        fmt.Printf("Peer is listening on: %d\n", info.peerListeningOn)
        log.Println(multiplexer.registerPeerAndWaitForError(*info, conn))
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

    info := HandshakeInfo { options.ListenPort }
    if err := sendHandshake(conn, info); err != nil {
        log.Fatal(err)
    }

    multiplexer := newMultiplexer()
    go multiplexer.driveDataStream(conn)
    log.Fatal(multiplexer.registerPeerAndWaitForError(info, streamOutput))
}
