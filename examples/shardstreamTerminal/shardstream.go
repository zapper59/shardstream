package main

import (
    "bufio"
    "github.com/akamensky/argparse"
    "github.com/zapper59/shardstream"
    "log"
    "os"
)

type ParsedArgs struct {
    listenAddress string
    coordinatorCommand *argparse.Command
    peerCommand *argparse.Command
    coordinatorAddress *string
}

func main() {
    parsedArgs := parseArgs()

    if parsedArgs.coordinatorCommand.Happened() {
        stdin := bufio.NewReader(os.Stdin)
        shards := shardstream.ShardCount(1)
        shardstream.RunCoordinator(
            stdin,
            shardstream.CoordinatorOptions{ shards, parsedArgs.listenAddress },
        )
    } else if parsedArgs.peerCommand.Happened() {
        shardstream.RunPeer(
            os.Stdout,
            shardstream.PeerOptions{ parsedArgs.listenAddress, *parsedArgs.coordinatorAddress },
        )
    } else {
        log.Fatal("Unexpected lack of subcommand!")
    }
}

func parseArgs() (ParsedArgs) {
    parser := argparse.NewParser(
        "shardstreamTerminal",
        "Start a node to participate in a distributed terminal based broadcast.",
    )
    listenAddress := parser.String(
        "l",
        "listenAddress",
        &argparse.Options{
            Required: true, Help: "The <hostname>:<port> to accept incoming requests on.",
        },
    )

    coordinator := parser.NewCommand("coordinator", "Start a root of a broadcast tree.")

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
        *listenAddress,
        coordinator,
        peer,
        coordinatorAddress,
    }
}
