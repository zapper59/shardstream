// Package shardstream defines a protocol for peer-to-peer torrent-like
// livestreaming.
// The shardstream protocol is Bittorrent for livestreams. It defines a general
// purpose framework for streaming data through a tree shaped network built of
// one Coordinator node and many Peer nodes.
package shardstream

import (
    "errors"
    "github.com/zapper59/abstractGoNet"
    "io"
    "iter"
    "log"
    "log/slog"
    "net"
    "sync"
)

// The settings to pass while creating a coordinator node's server.
type CoordinatorOptions struct {
    // The number of shards (1 or 2) this cluster will use to communicate.
    // The branching factor of the distribution tree will be equal 
    // to Shards + 1
    Shards ShardCount

    // A TCP listen address in the form of that accepted by [net.Listen].
    ListenAddress string
}

// The settings to pass while creating a peer node's server.
type PeerOptions struct {
    // A TCP listen address in the form of that accepted by [net.Listen].
    ListenAddress string

    // The address, in the form of that accepted by [net.Listen], of any node 
    // in the cluster that you wish to start your search at for inclusion in the
    // distribution tree. For an optimal distribution tree this should be the
    // node started using [shardstream.RunCoordinator]
    CoordinatorAddress string
}

// Start a coordinator node's server, broadcasting the data read from
// streamSource, which can be any [io.Reader] such as [os.Stdin]. The server will opportunistically perform reads for available
// data up to 2^16 bytes at a time. On any read error all processes for all
// nodes in the tree will return [io.EOF].
// Returns a function that will run the main process loop for the coordinator.
func StartCoordinator(streamSource io.Reader, options CoordinatorOptions, host abstractGoNet.Net) func () error {
    if options.Shards < 1 || options.Shards > 2 {
        log.Fatal("Only a shard count of 1 or 2 are supported.")
    }

    listener, err := host.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    shardIndices := shardIndices{ make(map[shardData]uint64) }
    s := firstShard
    for _ = range options.Shards {
        shardIndices.lastByteByShard[s] = 0
        s = s.nextShard(options.Shards)
    }
    server := newServer(options.Shards, shardIndices)

    return func () error {
        go server.driveServer(listener)
        return server.driveDataStream(newPaginator(streamSource))
    }
}

// Start a peer node's server, accepting the data stream as discovered from a
// DFS starting at the specified coordinator. The peer will consume an upload
// bandwidth equal to the branching factor configured by the coordinator.
// streamOutput will be handed a copy of the stream being broadcast and can be
// any [io.Writer] such as [os.Stdout].
// Returns a function that will run the main process loop for the peer.
func StartPeer(
    streamOutput io.Writer, options PeerOptions, host abstractGoNet.Net,
) func () error {
    listener, err := host.Listen("tcp", options.ListenAddress)
    if err != nil {
        log.Fatal(err)
    }

    slog.Debug("Beginning discovery.")
    info := handshake { 
        initiallyRequestedShardData,
        ListenAddress(options.ListenAddress),
    }
    discovery := runDiscovery(
        info, ListenAddress(options.CoordinatorAddress), host,
    )
    slog.Debug("Discovery completed.", "parents", discovery.parents)

    server := newServer(discovery.shards, discovery.shardIndices)

    return func () error {
        go server.runLocalPeer(streamOutput)
        go server.driveServer(listener)

        if len(discovery.parents) == 1 {
            conn := discovery.parents[everyShard(discovery.shards)]
            return server.driveDataStream(newPageReader(conn))
        } else if len(discovery.parents) == 2 {
            a := firstShard
            b := a.nextShard(discovery.shards)
            recombinated := newTwoShardRecombinator(
                newPageReader(discovery.parents[a]),
                discovery.shardIndices.lastByteByShard[a],
                newPageReader(discovery.parents[b]),
                discovery.shardIndices.lastByteByShard[b],
            )
            return server.driveDataStream(recombinated)
        } else {
            log.Fatalf("invalid discovery count of %d", len(discovery.parents))
            return nil
        }
    }
}

type server struct {
    shards ShardCount

    mutex sync.Mutex

    remotePeers remotePeerTable //< Mutex
    connectedPeers multiplexer //< Mutex
}

func newServer(shards ShardCount, shardIndices shardIndices) (server) {
    return server {
        shards: shards,
        remotePeers: newRemotePeerTable(shards),
        connectedPeers: newMultiplexer(shards, shardIndices),
    }
}

func (self *server) dropPeer(uid uint64) {
    if uid == maxUint64 {
        return
    }

    self.mutex.Lock()
    self.remotePeers.dropPeerLocked(uid)
    self.connectedPeers.dropPeerLocked(uid)
    self.mutex.Unlock()
}

func (self *server) driveServer(listener net.Listener) {
    defer listener.Close()

    for {
        if conn, err := listener.Accept(); err != nil {
            slog.Debug("Failed accept", "ERR", err)
        } else {
            go self.handleConnection(conn)
        }
    }
}

func (self *server) sendData(data pageData) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    self.connectedPeers.sendDataLocked(data)
}

func (self *server) driveDataStream(streamSource iter.Seq2[*pageData, error]) error {
    for page, err := range streamSource {
        if err != nil {
            return err
        }

        self.sendData(*page)
    }

    return nil
}

func (self *server) redirectPeerOrConnect(
    info handshake, streamOutput io.Writer, errorLog chan error,
) (uint64) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    connectedUid, redirectTable, nowServing :=
        self.remotePeers.redirectPeerOrConnectLocked(info)


    shardIndices := shardIndices{ make(map[shardData]uint64) }
    if nowServing != noShards {
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

    ack := handshakeAck { self.shards, redirectTable, shardIndices }
    slog.Debug(
        "Sending Ack",
        "redirect",
        ack.redirectTo.addressByShard,
        "nowServing",
        ack.nowServing.lastByteByShard,
    )


    // N.B. It is important that the ack is the first thing sent on this connection before releasing
    // the mutex which would allow subsequent data streaming.
    err := sendHandshakeAck(streamOutput, ack)
    if err != nil {
        slog.Debug("Failed to send ack", "ERR", err)
        errorLog <- err
    }

    return connectedUid
}

func (self *server) handleConnection(conn io.ReadWriteCloser) {
    defer conn.Close()

    info, err := receiveHandshake(conn)
    if err != nil {
        slog.Debug("Failed to accept handshake", "ERR", err)
        return
    }
    slog.Debug("Handshake", "hs", info)

    errorLog := make(chan error, 1)
    connectedUid := self.redirectPeerOrConnect(*info, conn, errorLog)
    defer self.dropPeer(connectedUid)
    err = <-errorLog
    slog.Debug("Connection Closed", "ERR", err)
}

func (self *server) connectLocalPeer(streamOutput io.Writer, errorLog chan error) (uint64) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    serveAllShards := everyShard(self.shards)
    connectedUid := maxUint64
    writer := newDepaginator(streamOutput)
    self.connectedPeers.registerConnectionLocked(
        serveAllShards, connectedUid, &writer, errorLog,
    )
    return connectedUid
}

func (self *server) runLocalPeer(streamOutput io.Writer) {
    errorLog := make(chan error, 1)
    connectedUid := self.connectLocalPeer(streamOutput, errorLog)
    defer self.dropPeer(connectedUid)
    err := <-errorLog
    slog.Debug("Local Peer Dropped", "ERR", err)
}
