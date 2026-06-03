package main

import (
    "bufio"
    "encoding/binary"
    "fmt"
    "github.com/akamensky/argparse"
    "io"
    "log"
    "net"
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
        runCoordinator(parsedArgs.listenPort)
    } else if parsedArgs.peerCommand.Happened() {
        runPeer(parsedArgs)
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

type HandshakeInfo struct {
    peerListeningOn int
}

func runCoordinator(port int) {
    portStr := fmt.Sprintf("%d", port)
    fmt.Println("Listening on port: " + portStr)
    listener, err := net.Listen("tcp", ":" + portStr)
    if err != nil {
        log.Fatal(err)
    }

    defer listener.Close()

    for {
        if conn, err := listener.Accept(); err != nil {
            log.Println(err)
        } else {
            go handleConnection(conn)
        }
    }
}

func handleConnection(conn net.Conn) {
    defer conn.Close()

    if info, err := receiveHandshake(conn); err != nil {
        log.Println(err)
    } else {
        fmt.Printf("Peer is listening on: %d", info.peerListeningOn)

        if _, err := conn.Write([]byte("Stream page 1\n")); err != nil {
            log.Println("Failed to stream page 1")
            log.Println(err)
        }
    }
}

func receiveHandshake(conn net.Conn) (*HandshakeInfo, error) {
    currentWord := make([]byte, 8)
    if _, err := io.ReadAtLeast(conn, currentWord, 8); err != nil {
        return nil, err
    }

    info := &HandshakeInfo { int(binary.BigEndian.Uint64(currentWord)) }
    return info, nil
}

func sendHandshake(conn net.Conn, info HandshakeInfo) (error) {
    currentWord := make([]byte, 8)
    binary.BigEndian.PutUint64(currentWord, uint64(info.peerListeningOn))
    _, err := conn.Write(currentWord)
    return err
}

func runPeer(args ParsedArgs) {
    conn, err := net.Dial("tcp", *args.coordinatorHost)
    if err != nil {
        log.Fatal(err)
    }

    if err := sendHandshake(conn, HandshakeInfo { args.listenPort } ); err != nil {
        log.Fatal(err)
    }

    page, err := bufio.NewReader(conn).ReadString('\n')
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(page)
}
