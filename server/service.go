package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/Eldrago12/DistributedFileSystem/gossip"
	pb "github.com/Eldrago12/DistributedFileSystem/proto"
	"github.com/Eldrago12/DistributedFileSystem/tcpconn"
)

const CHUNK_SIZE = 1024

type FileMeta struct {
	IsChunked  bool
	ChunkCount int
}

type FileStore struct {
	mu            sync.RWMutex
	files         map[string][]byte
	metadata      map[string]FileMeta
	gossipManager *gossip.Manager
}

func NewFileStore(gm *gossip.Manager) *FileStore {
	return &FileStore{
		files:         make(map[string][]byte),
		metadata:      make(map[string]FileMeta),
		gossipManager: gm,
	}
}

func ensureDir(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (fs *FileStore) SaveFile(filename string, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(data) <= CHUNK_SIZE {
		err := os.WriteFile(filepath.Join("data", filename), data, 0644)
		if err != nil {
			return err
		}
		fs.files[filename] = data
	} else {
		if err := ensureDir(filepath.Join("data", "chunks")); err != nil {
			return err
		}
		chunkCount := int(math.Ceil(float64(len(data)) / float64(CHUNK_SIZE)))
		loopSlice := make([]struct{}, chunkCount)
		for i := range loopSlice {
			start := i * CHUNK_SIZE
			end := min(start+CHUNK_SIZE, len(data))
			chunkData := data[start:end]
			chunkFilename := fmt.Sprintf("%s.%d", filename, i)
			err := os.WriteFile(filepath.Join("data", "chunks", chunkFilename), chunkData, 0644)
			if err != nil {
				return err
			}
			fs.replicateChunk(filename, i, chunkCount, chunkData)
		}
		fs.metadata[filename] = FileMeta{IsChunked: true, ChunkCount: chunkCount}
	}
	return nil
}

func (fs *FileStore) GetFile(filename string) ([]byte, error) {
	fs.mu.RLock()
	meta, chunked := fs.metadata[filename]
	fs.mu.RUnlock()

	if chunked && meta.IsChunked {
		var fullData []byte
		for i := 0; i < meta.ChunkCount; i++ {
			chunkFilename := fmt.Sprintf("%s.%d", filename, i)
			chunkPath := filepath.Join("data", "chunks", chunkFilename)
			chunkData, err := os.ReadFile(chunkPath)
			if err != nil {
				return nil, errors.New("file chunk not found")
			}
			fullData = append(fullData, chunkData...)
		}
		return fullData, nil
	}

	fs.mu.RLock()
	data, exists := fs.files[filename]
	fs.mu.RUnlock()
	if !exists {
		// Attempt to load from disk.
		fileData, err := os.ReadFile(filepath.Join("data", filename))
		if err != nil {
			return nil, errors.New("file not found")
		}
		fs.mu.Lock()
		fs.files[filename] = fileData
		fs.mu.Unlock()
		return fileData, nil
	}
	return data, nil
}

// replicateChunk sends a replication message for a file chunk to all peers.
func (fs *FileStore) replicateChunk(filename string, chunkIndex, chunkCount int, data []byte) {
	encodedData := base64.StdEncoding.EncodeToString(data)
	message := fmt.Sprintf("REPLICATE:%s:%d:%d:%s", filename, chunkIndex, chunkCount, encodedData)
	peers := fs.gossipManager.GetNodes()
	for _, peer := range peers {
		err := tcpconn.SendTCPMessage(peer, message)
		if err != nil {
			log.Printf("Replication to peer %s failed: %v", peer, err)
		} else {
			log.Printf("Replicated chunk %d of %s to peer %s", chunkIndex, filename, peer)
		}
	}
}

func (fs *FileStore) SaveChunk(filename string, chunkIndex, chunkCount int, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := ensureDir(filepath.Join("data", "chunks")); err != nil {
		return err
	}
	chunkFilename := fmt.Sprintf("%s.%d", filename, chunkIndex)
	err := os.WriteFile(filepath.Join("data", "chunks", chunkFilename), data, 0644)
	if err != nil {
		return err
	}
	if _, exists := fs.metadata[filename]; !exists {
		fs.metadata[filename] = FileMeta{IsChunked: true, ChunkCount: chunkCount}
	}
	return nil
}

type DFSServiceServer struct {
	pb.UnimplementedDFSServiceServer
	store *FileStore
}

func NewDFSServiceServer(store *FileStore) *DFSServiceServer {
	return &DFSServiceServer{store: store}
}

func (s *DFSServiceServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	err := s.store.SaveFile(req.Filename, req.Data)
	if err != nil {
		log.Printf("Failed to save file: %v", err)
		return &pb.UploadFileResponse{
			Success: false,
			Message: "Failed to save file",
		}, err
	}
	return &pb.UploadFileResponse{
		Success: true,
		Message: "File uploaded successfully",
	}, nil
}

func (s *DFSServiceServer) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
	data, err := s.store.GetFile(req.Filename)
	if err != nil {
		log.Printf("Failed to retrieve file: %v", err)
		return &pb.DownloadFileResponse{
			Success: false,
			Message: "File not found",
		}, err
	}
	return &pb.DownloadFileResponse{
		Success: true,
		Message: "File retrieved successfully",
		Data:    data,
	}, nil
}
