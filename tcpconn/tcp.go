package tcpconn

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

var ReplicationHandler func(message string)
var GossipHandler func(message string)

func StartTCPServer(address string) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Error starting TCP server: %v", err)
	}
	log.Printf("TCP server started on %s", address)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go handleConnection(conn)
		}
	}()
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		message := scanner.Text()
		log.Printf("Received TCP message: %s", message)
		if strings.HasPrefix(message, "REPLICATE:") {
			if ReplicationHandler != nil {
				ReplicationHandler(message)
			}
		} else if strings.HasPrefix(message, "GOSSIP|") {
			if GossipHandler != nil {
				GossipHandler(message)
			}
		} else {
			log.Printf("Unhandled message: %s", message)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v", err)
	}
}

func SendTCPMessage(address, message string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("error connecting to %s: %v", address, err)
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "%s\n", message)
	if err != nil {
		return fmt.Errorf("error sending message to %s: %v", address, err)
	}
	return nil
}
