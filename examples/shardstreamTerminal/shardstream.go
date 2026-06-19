package main

import (
    "bufio"
    "github.com/akamensky/argparse"
    "github.com/zapper59/abstractGoNet"
    "github.com/zapper59/shardstream"
    "log"
    "log/slog"
    "os"
)

type ParsedArgs struct {
    debugLogging bool
    listenAddress string

    coordinatorCommand *argparse.Command
    shardCount *int

    peerCommand *argparse.Command
    coordinatorAddress *string
}

func main() {
    parsedArgs := parseArgs()

    if parsedArgs.debugLogging {
        slog.SetLogLoggerLevel(slog.LevelDebug)
    }

    host := abstractGoNet.RealNet()

    if parsedArgs.coordinatorCommand.Happened() {
        stdin := bufio.NewReader(os.Stdin)
        shards := shardstream.ShardCount(*parsedArgs.shardCount)
        runCoordinator := shardstream.StartCoordinator(
            stdin,
            shardstream.CoordinatorOptions{ shards, parsedArgs.listenAddress },
            host,
        )
        runCoordinator()
    } else if parsedArgs.peerCommand.Happened() {
        runPeer := shardstream.StartPeer(
            os.Stdout,
            shardstream.PeerOptions{ parsedArgs.listenAddress, *parsedArgs.coordinatorAddress },
            host,
        )
        runPeer()
    } else {
        log.Fatal("Unexpected lack of subcommand!")
    }
}

func parseArgs() (ParsedArgs) {
    parser := argparse.NewParser(
        "shardstreamTerminal",
        "Start a node to participate in a distributed terminal based broadcast.",
    )
    debugLogging := parser.Flag("v", "verbose", &argparse.Options{})

    listenAddress := parser.String(
        "l",
        "listenAddress",
        &argparse.Options{
            Required: true, Help: "The <hostname>:<port> to accept incoming requests on.",
        },
    )

    coordinator := parser.NewCommand("coordinator", "Start a root of a broadcast tree.")
    shardCount := coordinator.Int(
        "s",
        "shardCount",
        &argparse.Options{
            Required: false, 
            Default: 2,
            Help: "The number of shards to split data into. (1 or 2)",
        },
    )

    peer := parser.NewCommand("peer", "Start a participant in a broadcast tree.")
    coordinatorAddress := peer.String(
        "c",
        "coordinatorAddress",
        &argparse.Options{
            Required: true, Help: "The <hostname>:<port> to contact the coordinator at.",
        },
    )

    if err := parser.Parse(os.Args); err != nil {
        log.Fatal(parser.Usage(err))
    }

    return ParsedArgs{
        *debugLogging,
        *listenAddress,
        coordinator,
        shardCount,
        peer,
        coordinatorAddress,
    }
}
