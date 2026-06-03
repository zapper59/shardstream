package main

import (
    "bufio"
    "github.com/akamensky/argparse"
    "github.com/zapper59/shardstream"
    "log"
    "os"
)

type ParsedArgs struct {
    listenPort int
    coordinatorCommand *argparse.Command
    peerCommand *argparse.Command
    coordinatorHost *string
}

func main() {
    parsedArgs := parseArgs()

    if parsedArgs.coordinatorCommand.Happened() {
        stdin := bufio.NewReader(os.Stdin)
        shardstream.RunCoordinator(stdin, parsedArgs.listenPort)
    } else if parsedArgs.peerCommand.Happened() {
        shardstream.RunPeer(
            os.Stdout,
            shardstream.PeerOptions{ parsedArgs.listenPort, *parsedArgs.coordinatorHost },
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
    listenPort := parser.Int(
        "l",
        "listenPort",
        &argparse.Options{Required: true, Help: "The port to accept incoming requests on."},
    )

    coordinator := parser.NewCommand("coordinator", "Start a root of a broadcast tree.")

    peer := parser.NewCommand("peer", "Start a participant in a broadcast tree.")
    coordinatorHost := peer.String(
        "c",
        "coordinatorHost",
        &argparse.Options{
            Required: true, Help: "The <hostname>:<port> to contact the coordinator at.",
        },
    )

    if err := parser.Parse(os.Args); err != nil {
        log.Fatal(parser.Usage(err))
    }

    return ParsedArgs{*listenPort, coordinator, peer, coordinatorHost}
}
