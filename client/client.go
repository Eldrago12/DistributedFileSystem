package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"

	pb "github.com/Eldrago12/DistributedFileSystem/proto"
)

func main() {
	// Command-line flags:
	// -server: the gRPC server address to connect to.
	// -download: filename to download from the DFS.
	// -out: output file path for downloaded data.
	serverAddr := flag.String("server", "localhost:50051", "gRPC server address (host:port)")
	uploadPath := flag.String("upload", "", "Path to local file to upload")
	downloadFile := flag.String("download", "", "Filename to download from DFS")
	outPath := flag.String("out", "downloaded_file", "Output file path for downloaded file")
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", *serverAddr, err)
	}
	defer conn.Close()
	client := pb.NewDFSServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if *uploadPath != "" {
		data, err := os.ReadFile(*uploadPath)
		if err != nil {
			log.Fatalf("Failed to read file %s: %v", *uploadPath, err)
		}
		req := &pb.UploadFileRequest{
			Filename: "uploaded_" + getFileName(*uploadPath),
			Data:     data,
		}
		res, err := client.UploadFile(ctx, req)
		if err != nil {
			log.Fatalf("UploadFile error: %v", err)
		}
		fmt.Printf("UploadFile response: %s\n", res.Message)
	}

	if *downloadFile != "" {
		req := &pb.DownloadFileRequest{
			Filename: *downloadFile,
		}
		res, err := client.DownloadFile(ctx, req)
		if err != nil {
			log.Fatalf("DownloadFile error: %v", err)
		}
		if !res.Success {
			log.Fatalf("DownloadFile failed: %s", res.Message)
		}
		err = os.WriteFile(*outPath, res.Data, 0644)
		if err != nil {
			log.Fatalf("Error writing file to %s: %v", *outPath, err)
		}
		fmt.Printf("File downloaded and saved to %s\n", *outPath)
	}

	if *uploadPath == "" && *downloadFile == "" {
		fmt.Println("Please provide either -upload or -download flag.")
		flag.Usage()
		os.Exit(1)
	}
}

func getFileName(path string) string {
	parts := []rune(path)
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] == '/' || parts[i] == '\\' {
			return string(parts[i+1:])
		}
	}
	return path
}
