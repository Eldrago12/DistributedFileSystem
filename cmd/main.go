package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Eldrago12/DistributedFileSystem/gossip"
	pb "github.com/Eldrago12/DistributedFileSystem/proto"
	"github.com/Eldrago12/DistributedFileSystem/server"
	"github.com/Eldrago12/DistributedFileSystem/tcpconn"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "gRPC server port")
	tcpPort := flag.Int("tcpPort", 6000, "TCP server port for gossip/replication")
	peerAddr := flag.String("peer", "", "Peer address to join (for gossip)")
	flag.Parse()

	if err := os.MkdirAll("data", os.ModePerm); err != nil {
		log.Fatalf("Could not create data directory: %v", err)
	}

	tcpAddress := fmt.Sprintf(":%d", *tcpPort)
	tcpconn.StartTCPServer(tcpAddress)

	selfAddress := fmt.Sprintf("localhost:%d", *tcpPort)
	gossipManager := gossip.NewManager(selfAddress)
	if *peerAddr != "" {
		gossipManager.AddNode(*peerAddr)
	}

	fs := server.NewFileStore(gossipManager)

	tcpconn.ReplicationHandler = func(message string) {
		parts := strings.SplitN(message, ":", 5)
		if len(parts) != 5 {
			log.Printf("Invalid replication message format: %s", message)
			return
		}
		filename := parts[1]
		chunkIndex, err := strconv.Atoi(parts[2])
		if err != nil {
			log.Printf("Error parsing chunkIndex: %v", err)
			return
		}
		chunkCount, err := strconv.Atoi(parts[3])
		if err != nil {
			log.Printf("Error parsing chunkCount: %v", err)
			return
		}
		data, err := base64.StdEncoding.DecodeString(parts[4])
		if err != nil {
			log.Printf("Error decoding chunk data: %v", err)
			return
		}
		err = fs.SaveChunk(filename, chunkIndex, chunkCount, data)
		if err != nil {
			log.Printf("Error saving replicated chunk: %v", err)
		} else {
			log.Printf("Replicated chunk %d of file '%s' saved successfully", chunkIndex, filename)
		}
	}

	tcpconn.GossipHandler = func(message string) {
		parts := strings.SplitN(message, "|", 4)
		if len(parts) < 4 {
			log.Printf("Invalid gossip message format: %s", message)
			return
		}
		senderAddress := parts[1]
		gossipManager.AddNode(senderAddress)
		gossipManager.UpdateNodeHeartbeat(senderAddress)
		peerListStr := parts[3]
		peerListStr = strings.Trim(peerListStr, "[]")
		peerAddresses := strings.Fields(peerListStr)
		for _, addr := range peerAddresses {
			if addr != "" && addr != gossipManager.SelfAddress() {
				gossipManager.AddNode(addr)
			}
		}
		log.Printf("Processed gossip from %s: peers %v", senderAddress, peerAddresses)
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			state := gossipManager.GetState()
			log.Println("Current gossip manager state:")
			for addr, node := range state {
				log.Printf("Node: %s, Healthy: %t, LastHeartbeat: %v, Files: %v", addr, node.Healthy, node.LastHeartbeat, node.FileList)
			}
		}
	}()

	go gossipManager.Gossip()

	grpcAddress := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", grpcAddress, err)
	}

	grpcServer := grpc.NewServer()
	dfsService := server.NewDFSServiceServer(fs)
	pb.RegisterDFSServiceServer(grpcServer, dfsService)

	log.Printf("gRPC server listening on %s", grpcAddress)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
