package shardstream

import (
    "bufio"
    "encoding/binary"
    "errors"
    "io"
    "log"
    "net"
    "strings"
    "sync"
)

type PeerOptions struct {
    ListenAddress string
    CoordinatorAddress string
}

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

const ReadBufferSize = 1024 * 1024
const BranchingFactor = 2 // The numer of non-local peers to allow before sending redirects.
const MaxUint64 = ^uint64(0)

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
    childPeers uint64
    info *HandshakeInfo // N.B. info is nil IFF the peer is a local consumer of data, Ie. does not
                        // count against the network branching factor.
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
    if uid == MaxUint64 {
        return
    }

    self.mutex.Lock()
    delete(self.connectedPeers, uid)
    self.mutex.Unlock()
}

func (self *Multiplexer) computeRedirectLocked() (HandshakeAck, string) {
    ack := HandshakeAck { make(map[ShardData]HandshakeInfo) }

    minChildPeers := MaxUint64
    optimalPeerAddress := "invalid_hostname"
    optimalPeerUID := MaxUint64
    for uid, peer := range self.connectedPeers {
        if peer.info != nil && peer.childPeers < minChildPeers {
            minChildPeers = peer.childPeers
            optimalPeerAddress = peer.info.peerListeningOn
            optimalPeerUID = uid
        }
    }
    tempPeer := self.connectedPeers[optimalPeerUID]
    tempPeer.childPeers += 1
    self.connectedPeers[optimalPeerUID] = tempPeer

    ack.redirectTo[AB] = HandshakeInfo { AB, optimalPeerAddress }
    return ack, optimalPeerAddress
}

func (self *Multiplexer) connectPeerLocked(
    info *HandshakeInfo, streamOutput io.Writer, peerErrorLog chan error,
) (uint64) {
    self.peerUIDAllocator += 1
    connectedUid := self.peerUIDAllocator

    self.connectedPeers[connectedUid] = ConnectedPeer {
        0, // Start with no childPeers.
        connectedUid,
        info,
        streamOutput,
        peerErrorLog,
    }

    return connectedUid
}

func (self *Multiplexer) redirectOrConnectPeer(
    info *HandshakeInfo, streamOutput io.Writer, peerErrorLog chan error,
) (uint64) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    connectedUid := MaxUint64

    remotePeers := 0
    for _, peer := range self.connectedPeers {
        if peer.info != nil {
            remotePeers++
        }
    }

    ack := HandshakeAck { make(map[ShardData]HandshakeInfo) }

    if info != nil && remotePeers >= BranchingFactor {
        redirectAck, optimalPeerAddress := self.computeRedirectLocked()
        ack = redirectAck
        peerErrorLog <- errors.New("Redirect to: " + optimalPeerAddress)
    } else {
        connectedUid = self.connectPeerLocked(info, streamOutput, peerErrorLog)
    }

    // N.B. It is important that the ack is the first thing sent on this connection before releasing
    // the mutex and allowing subsequent data streaming.
    if info != nil {
        err := sendHandshakeAck(streamOutput, ack)
        if err != nil {
            log.Println(err)
        }
    }

    return connectedUid
}

func (self *Multiplexer) redirectPeerOrWaitForError(
    info *HandshakeInfo, streamOutput io.Writer,
) (error) {
    errorLog := make(chan error, 1)

    connectedUid := self.redirectOrConnectPeer(info, streamOutput, errorLog)
    defer self.dropPeer(connectedUid)

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
