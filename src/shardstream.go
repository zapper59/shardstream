package shardstream

import (
    "bufio"
    "encoding/binary"
    "io"
    "log"
    "net"
    "sync"
)

type PeerOptions struct {
    ListenAddress string
    CoordinatorHost string
}

type HandshakeInfo struct {
    peerListeningOn string
}

const ReadBufferSize = 1024 * 1024

// Indicates a request for a full data stream. Ie. a non-sharded data source.
const AB = 3

func RunCoordinator(streamSource io.Reader, listenAddress string) {
    listener, err := net.Listen("tcp", listenAddress)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    multiplexer := newMultiplexer()
    go multiplexer.driveDataStream(streamSource)
    multiplexer.driveServer(listener)
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

func (self *Multiplexer) driveServer(listener net.Listener) {
    for {
        if conn, err := listener.Accept(); err != nil {
            log.Println(err)
        } else {
            go self.handleConnection(conn)
        }
    }
}

func (self *Multiplexer) handleConnection(conn net.Conn) {
    defer conn.Close()

    if info, err := receiveHandshake(conn); err != nil {
        log.Println(err)
    } else {
        log.Println(self.registerPeerAndWaitForError(*info, conn))
    }
}

func receiveHandshake(conn net.Conn) (*HandshakeInfo, error) {
    currentWord := make([]byte, 8)

    // For now, throw away the shard info.
    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }

    peerListeningOn, err := bufio.NewReader(conn).ReadString(0)
    if err != nil {
        return nil, err
    }
    info := &HandshakeInfo { peerListeningOn }
    return info, nil
}

func sendHandshake(conn net.Conn, info HandshakeInfo) (error) {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(AB))
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

func RunPeer(streamOutput io.Writer, options PeerOptions) {
    listener, err := net.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    conn, err := net.Dial("tcp", options.CoordinatorHost)
    if err != nil {
        log.Fatal(err)
    }

    info := HandshakeInfo { options.ListenAddress }
    if err := sendHandshake(conn, info); err != nil {
        log.Fatal(err)
    }

    multiplexer := newMultiplexer()
    go multiplexer.driveDataStream(conn)
    go multiplexer.driveServer(listener)
    log.Fatal(multiplexer.registerPeerAndWaitForError(info, streamOutput))
}
