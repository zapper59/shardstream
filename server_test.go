package shardstream

import (
    "errors"
    "github.com/zapper59/abstractGoNet"
    "io"
    "os"
    "testing"
)

func TestCoordinatorNoPeersOneShard(t *testing.T) {
    wan := abstractGoNet.NewVirtualWan()
    host := wan.NewVirtualHost("coordinator")
    shards := ShardCount(1)
    runCoordinator := StartCoordinator(
        os.Stdin,
        CoordinatorOptions{ shards, ":8080" },
        host,
    )
    if err := runCoordinator(); !errors.Is(err, io.EOF) {
        t.Error(err)
    }
}

func TestCoordinatorNoPeersTwoShards(t *testing.T) {
    wan := abstractGoNet.NewVirtualWan()
    host := wan.NewVirtualHost("coordinator")
    shards := ShardCount(2)
    runCoordinator := StartCoordinator(
        os.Stdin,
        CoordinatorOptions{ shards, ":8080" },
        host,
    )
    if err := runCoordinator(); !errors.Is(err, io.EOF) {
        t.Error(err)
    }
}

func TestCoordinatorZeroShards(t *testing.T) {
    wan := abstractGoNet.NewVirtualWan()
    host := wan.NewVirtualHost("coordinator")
    shards := ShardCount(0)
    runCoordinator := StartCoordinator(
        os.Stdin,
        CoordinatorOptions{ shards, ":8080" },
        host,
    )
    if err := runCoordinator(); !errors.Is(err, InvalidShardCount) {
        t.Error(err)
    }
}

func TestCoordinatorThreeShards(t *testing.T) {
    wan := abstractGoNet.NewVirtualWan()
    host := wan.NewVirtualHost("coordinator")
    shards := ShardCount(3)
    runCoordinator := StartCoordinator(
        os.Stdin,
        CoordinatorOptions{ shards, ":8080" },
        host,
    )
    if err := runCoordinator(); !errors.Is(err, InvalidShardCount) {
        t.Error(err)
    }
}

func TestPeerWithNoCoordinator(t *testing.T) {
    wan := abstractGoNet.NewVirtualWan()
    host := wan.NewVirtualHost("peer")
    runPeer := StartPeer(
        os.Stdout,
        PeerOptions{ ":8080", "invalid:8080" },
        host,
    )
    if err := runPeer(); !errors.Is(err, abstractGoNet.HostNotFoundErr) {
        t.Error(err)
    }
}
