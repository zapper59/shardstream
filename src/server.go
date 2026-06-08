package shardstream

import (
    "errors"
    "io"
    "log"
    "net"
    "sync"
)

type PeerOptions struct {
    ListenAddress string
    CoordinatorAddress string
}

const MaxUint64 = ^uint64(0)

func RunCoordinator(streamSource io.Reader, listenAddress string) {
    listener, err := net.Listen("tcp", listenAddress)
    if err != nil {
        log.Fatal(err)
    }

    server := newServer()
    go server.driveServer(listener)
    server.driveDataStream(streamSource)
}

func RunPeer(streamOutput io.Writer, options PeerOptions) {
    listener, err := net.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    info := HandshakeInfo { AB, options.ListenAddress }
    conn := runDiscovery(info, options.CoordinatorAddress)

    server := newServer()
    go server.runLocalPeer(streamOutput)
    go server.driveServer(listener)
    server.driveDataStream(conn)
}

type Server struct {
    mutex sync.Mutex

    remotePeers RemotePeerTable
    connectedPeers Multiplexer
}

func newServer() (Server) {
    return Server {
        remotePeers: newRemotePeerTable(),
        connectedPeers: newMultiplexer(),
    }
}

func (self *Server) dropPeer(uid uint64) {
    if uid == MaxUint64 {
        return
    }

    self.mutex.Lock()
    self.remotePeers.dropPeerLocked(uid)
    self.connectedPeers.dropPeerLocked(uid)
    self.mutex.Unlock()
}

func (self *Server) driveServer(listener net.Listener) {
    defer listener.Close()

    for {
        if conn, err := listener.Accept(); err != nil {
            log.Println(err)
        } else {
            go self.handleConnection(conn)
        }
    }
}

func (self *Server) sendData(data []byte) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    self.connectedPeers.sendDataLocked(data)
}

func (self *Server) driveDataStream(streamSource io.Reader) {
    readBuffer := make([]byte, ReadBufferSize)

    for {
        bytesRead, err := streamSource.Read(readBuffer)
        if err != nil {
            log.Println(err)
            return
        }

        self.sendData(readBuffer[:bytesRead])
    }
}

func (self *Server) redirectPeerOrConnect(
    info HandshakeInfo, streamOutput io.Writer, errorLog chan error,
) (uint64) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    connectedUid, ack := self.remotePeers.redirectPeerOrConnectLocked(info)

    // N.B. It is important that the ack is the first thing sent on this connection before releasing
    // the mutex which would allow subsequent data streaming.
    err := sendHandshakeAck(streamOutput, ack)
    if err != nil {
        log.Println(err)
    }

    if len(ack.redirectTo) == 0 {
        self.connectedPeers.registerConnectionLocked(connectedUid, streamOutput, errorLog)
    } else {
        errorLog <- errors.New("Redirect to: " + ack.redirectTo[AB].peerListeningOn)
    }

    return connectedUid
}

func (self *Server) handleConnection(conn net.Conn) {
    defer conn.Close()

    info, err := receiveHandshake(conn)
    if err != nil {
        log.Println(err)
        return
    }

    errorLog := make(chan error, 1)
    connectedUid := self.redirectPeerOrConnect(*info, conn, errorLog)
    defer self.dropPeer(connectedUid)
    err = <-errorLog
    log.Println(err)
}

func (self *Server) connectLocalPeer(streamOutput io.Writer, errorLog chan error) (uint64) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    connectedUid := MaxUint64
    self.connectedPeers.registerConnectionLocked(connectedUid, streamOutput, errorLog)
    return connectedUid
}

func (self *Server) runLocalPeer(streamOutput io.Writer) {
    errorLog := make(chan error, 1)
    connectedUid := self.connectLocalPeer(streamOutput, errorLog)
    defer self.dropPeer(connectedUid)
    err := <-errorLog
    log.Println(err)
}
