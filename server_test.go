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

const someData = "some data"

func doOnePeerTest(shards ShardCount, t *testing.T) {
    wan := abstractGoNet.NewVirtualWan()
    host1 := wan.NewVirtualHost("coordinator")

    inR, inW := io.Pipe()
    runCoordinator := StartCoordinator(
        inR,
        CoordinatorOptions{ shards, ":8080" },
        host1,
    )
    go runCoordinator()

    outR, outW := io.Pipe()
    host2 := wan.NewVirtualHost("peer")
    runPeer := StartPeer(
        outW,
        PeerOptions{ ":8080", "coordinator:8080" },
        host2,
    )
    go runPeer()

    for _ = range 10 {
        size, err := io.WriteString(inW, someData)
        if err != nil {
            t.Error(err)
        }

        buff := make([]byte, size)
        _, err = io.ReadFull(outR, buff)
        if err != nil {
            t.Error(err)
        }

        if string(buff) != someData {
            t.Errorf("invalid buff")
        }
    }
}

func TestOnePeerOneShard(t *testing.T) {
    doOnePeerTest(ShardCount(1), t)
}

func TestOnePeerTwoShards(t *testing.T) {
    doOnePeerTest(ShardCount(2), t)
}
