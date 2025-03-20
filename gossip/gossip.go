package gossip

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Eldrago12/DistributedFileSystem/tcpconn"
)

type Node struct {
	Address       string
	Healthy       bool
	LastHeartbeat time.Time
	FileList      []string
}

type Manager struct {
	mu    sync.Mutex
	Nodes map[string]*Node
	self  *Node
}

func NewManager(selfAddress string) *Manager {
	return &Manager{
		Nodes: make(map[string]*Node),
		self: &Node{
			Address:       selfAddress,
			Healthy:       true,
			LastHeartbeat: time.Now(),
			FileList:      []string{},
		},
	}
}

func (gm *Manager) AddNode(address string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, exists := gm.Nodes[address]; !exists && address != gm.self.Address {
		gm.Nodes[address] = &Node{
			Address:       address,
			Healthy:       true,
			LastHeartbeat: time.Now(),
			FileList:      []string{},
		}
		log.Printf("Node added: %s\n", address)
	}
}

func (gm *Manager) UpdateNodeHeartbeat(address string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if node, exists := gm.Nodes[address]; exists {
		node.LastHeartbeat = time.Now()
		node.Healthy = true
		log.Printf("Updated heartbeat for node %s: %v", address, node.LastHeartbeat)
	}
}

func (gm *Manager) SetNodeUnhealthy(address string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if node, exists := gm.Nodes[address]; exists {
		node.Healthy = false
		log.Printf("Set node %s as unhealthy", address)
	}
}

func (gm *Manager) GetState() map[string]Node {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	state := make(map[string]Node)
	for addr, node := range gm.Nodes {
		state[addr] = *node
	}
	return state
}

func (gm *Manager) Gossip() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		gm.mu.Lock()
		var nodeList []string
		for addr, node := range gm.Nodes {
			if node.Healthy {
				nodeList = append(nodeList, addr)
			}
		}
		gm.mu.Unlock()

		if len(nodeList) > 0 {
			peer := nodeList[rand.Intn(len(nodeList))]
			message := fmt.Sprintf("GOSSIP|%s|%d|%v", gm.self.Address, time.Now().Unix(), nodeList)
			err := tcpconn.SendTCPMessage(peer, message)
			if err != nil {
				log.Printf("Error gossiping to peer %s: %v", peer, err)
			} else {
				log.Printf("Gossiped to peer %s", peer)
			}
		} else {
			log.Println("No healthy peers found to gossip with.")
		}
	}
}

func (gm *Manager) GetNodes() []string {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	var nodes []string
	for addr := range gm.Nodes {
		nodes = append(nodes, addr)
	}
	return nodes
}

func (gm *Manager) SelfAddress() string {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	return gm.self.Address
}
