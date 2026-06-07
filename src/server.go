package shardstream

import (
    "io"
    "log"
    "net"
    "sync"
)

type PeerOptions struct {
    ListenAddress string
    CoordinatorAddress string
}

const MaxUint16 = ^uint16(0)
const MaxUint64 = ^uint64(0)
const ReadBufferSize = MaxUint16
const BranchingFactor = 2 // The numer of non-local peers to allow before sending redirects.

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
    streamOutput io.Writer
    errorLog chan error
}

type Multiplexer struct {
    mutex sync.Mutex

    remotePeers RemotePeerTable
    connectedPeers map[uint64]ConnectedPeer
}

func newMultiplexer() (Multiplexer) {
    return Multiplexer {
        remotePeers: newRemotePeerTable(),
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
    if uid == MaxUint64 {
        return
    }

    self.mutex.Lock()
    self.remotePeers.dropPeerLocked(uid)
    delete(self.connectedPeers, uid)
    self.mutex.Unlock()
}

func (self *Multiplexer) redirectPeerOrWaitForError(
    info *HandshakeInfo, streamOutput io.Writer,
) (error) {
    errorLog := make(chan error, 1)

    self.mutex.Lock()

    connectedUid, ack := self.remotePeers.redirectOrConnectPeerLocked(
        info, streamOutput, errorLog,
    )
    defer self.dropPeer(connectedUid)

    // N.B. It is important that the ack is the first thing sent on this connection before releasing
    // the mutex and allowing subsequent data streaming.
    if info != nil {
        err := sendHandshakeAck(streamOutput, ack)
        if err != nil {
            log.Println(err)
        }
    }

    if len(ack.redirectTo) == 0 {
        self.connectedPeers[connectedUid] = ConnectedPeer { connectedUid, streamOutput, errorLog }
    }

    self.mutex.Unlock()

    return <-errorLog
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

    info, err := receiveHandshake(conn)
    if err != nil {
        log.Println(err)
        return
    }

    log.Println(self.redirectPeerOrWaitForError(info, conn))
}

func runDiscovery(info HandshakeInfo, host string) (net.Conn){
    conn, err := net.Dial("tcp", host)
    if err != nil {
        log.Fatal(err)
    }

    if err := sendHandshake(conn, info); err != nil {
        log.Fatal(err)
    }

    ack, err := receiveHandshakeAck(conn)
    if err != nil {
        log.Fatal(err)
    }

    if len(ack.redirectTo) == 0 {
        return conn
    } else {
        conn.Close()
        return runDiscovery(info, ack.redirectTo[AB].peerListeningOn)
    }
}

func RunPeer(streamOutput io.Writer, options PeerOptions) {
    listener, err := net.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    info := HandshakeInfo { AB, options.ListenAddress }
    conn := runDiscovery(info, options.CoordinatorAddress)

    multiplexer := newMultiplexer()
    go multiplexer.driveDataStream(conn)
    go multiplexer.driveServer(listener)
    log.Fatal(multiplexer.redirectPeerOrWaitForError(nil, streamOutput))
}
