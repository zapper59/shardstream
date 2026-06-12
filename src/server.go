package shardstream

import (
    "errors"
    "io"
    "iter"
    "log"
    "net"
    "sync"
)

type CoordinatorOptions struct {
    Shards ShardCount
    ListenAddress string
}

type PeerOptions struct {
    ListenAddress string
    CoordinatorAddress string
}

const MaxUint64 = ^uint64(0)

func RunCoordinator(streamSource io.Reader, options CoordinatorOptions) {
    listener, err := net.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    shardIndices := ShardIndices{ make(map[ShardData]uint64) }
    shardIndices.lastByteByShard[FirstShard] = 0
    server := newServer(options.Shards, shardIndices)
    go server.driveServer(listener)
    server.driveDataStream(newPaginator(streamSource))
}

func RunPeer(streamOutput io.Writer, options PeerOptions) {
    listener, err := net.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    info := Handshake { ListenAddress(options.ListenAddress) }
    conn, shards, shardIndices := runDiscovery(info, options.CoordinatorAddress)

    server := newServer(shards, shardIndices)
    go server.runLocalPeer(streamOutput)
    go server.driveServer(listener)
    server.driveDataStream(newPageReader(conn))
}

type Server struct {
    shards ShardCount

    mutex sync.Mutex

    remotePeers RemotePeerTable //< Mutex
    connectedPeers Multiplexer //< Mutex
}

func newServer(shards ShardCount, shardIndices ShardIndices) (Server) {
    return Server {
        shards: shards,
        remotePeers: newRemotePeerTable(shards),
        connectedPeers: newMultiplexer(shardIndices),
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

func (self *Server) sendData(data PageData) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    self.connectedPeers.sendDataLocked(data)
}

func (self *Server) driveDataStream(streamSource iter.Seq2[*PageData, error]) {
    for page, err := range streamSource {
        if err != nil {
            log.Println(err)
            return
        }

        self.sendData(*page)
    }
}

func (self *Server) redirectPeerOrConnect(
    info Handshake, streamOutput io.Writer, errorLog chan error,
) (uint64) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    connectedUid, redirectTable, nowServing :=
        self.remotePeers.redirectPeerOrConnectLocked(info)


    shardIndices := ShardIndices{ make(map[ShardData]uint64) }
    if nowServing != NoShards {
        writer := newPageSerializer(streamOutput)
        shardIndices = self.connectedPeers.registerConnectionLocked(
            nowServing, connectedUid, &writer, errorLog,
        )
    } else {
        redirectShardData := everyShard(self.shards)
        errorLog <- errors.New(
            "Redirect to: " + 
            string(redirectTable.addressByShard[redirectShardData]),
        )
    }

    ack := HandshakeAck { self.shards, redirectTable, shardIndices }

    // N.B. It is important that the ack is the first thing sent on this connection before releasing
    // the mutex which would allow subsequent data streaming.
    err := sendHandshakeAck(streamOutput, ack)
    if err != nil {
        log.Println(err)
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

    serveAllShards := everyShard(self.shards)
    connectedUid := MaxUint64
    writer := newDepaginator(streamOutput)
    self.connectedPeers.registerConnectionLocked(
        serveAllShards, connectedUid, &writer, errorLog,
    )
    return connectedUid
}

func (self *Server) runLocalPeer(streamOutput io.Writer) {
    errorLog := make(chan error, 1)
    connectedUid := self.connectLocalPeer(streamOutput, errorLog)
    defer self.dropPeer(connectedUid)
    err := <-errorLog
    log.Println(err)
}
