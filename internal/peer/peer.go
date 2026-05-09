// Package peer implements the core peer node logic:
// joining the network, discovering other peers, gossiping,
// and downloading files from peers.
package peer

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"p2p/internal/protocol"
)

const (
	heartbeatInterval = 20 * time.Second
	gossipInterval    = 30 * time.Second
	dialTimeout       = 5 * time.Second
)

// Node is a peer in the P2P network.
type Node struct {
	ID           string
	Addr         string // our own host:port (file server address)
	DownloadDir  string
	DiscoveryAddrs []string // list of known discovery nodes

	mu    sync.RWMutex
	peers map[string]protocol.PeerInfo // discovered peers
}

func New(id, addr, downloadDir string, discoveryAddrs []string) *Node {
	return &Node{
		ID:             id,
		Addr:           addr,
		DownloadDir:    downloadDir,
		DiscoveryAddrs: discoveryAddrs,
		peers:          make(map[string]protocol.PeerInfo),
	}
}

// Start registers with discovery nodes and begins background tasks.
func (n *Node) Start() {
	n.registerWithAll()
	go n.heartbeatLoop()
	go n.gossipLoop()
}

// registerWithAll tries to register with every configured discovery node.
func (n *Node) registerWithAll() {
	for _, addr := range n.DiscoveryAddrs {
		peers, err := n.registerWith(addr)
		if err != nil {
			log.Printf("[peer] could not reach discovery %s: %v", addr, err)
			continue
		}
		n.mergePeers(peers)
		log.Printf("[peer] registered with discovery %s, got %d peers", addr, len(peers))
	}
}

func (n *Node) registerWith(discoveryAddr string) ([]protocol.PeerInfo, error) {
	conn, err := net.DialTimeout("tcp", discoveryAddr, dialTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := protocol.Send(conn, protocol.MsgRegister, protocol.RegisterPayload{
		ID:   n.ID,
		Addr: n.Addr,
	}); err != nil {
		return nil, err
	}

	env, err := protocol.Recv(conn, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if env.Type == protocol.MsgError {
		var e protocol.ErrorPayload
		protocol.Decode(env, &e)
		return nil, fmt.Errorf("discovery error: %s", e.Message)
	}
	if env.Type != protocol.MsgPeerList {
		return nil, fmt.Errorf("unexpected response: %s", env.Type)
	}
	var pl protocol.PeerListPayload
	if err := protocol.Decode(env, &pl); err != nil {
		return nil, err
	}
	return pl.Peers, nil
}

// heartbeatLoop periodically re-registers with discovery nodes.
func (n *Node) heartbeatLoop() {
	for range time.Tick(heartbeatInterval) {
		n.registerWithAll()
	}
}

// gossipLoop shares peer knowledge with random peers to maintain resilience.
func (n *Node) gossipLoop() {
	for range time.Tick(gossipInterval) {
		n.gossip()
	}
}

func (n *Node) gossip() {
	n.mu.RLock()
	peerList := make([]protocol.PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		peerList = append(peerList, p)
	}
	n.mu.RUnlock()

	if len(peerList) == 0 {
		return
	}

	// Pick a random peer to gossip with
	target := peerList[rand.Intn(len(peerList))]
	if target.ID == n.ID {
		return
	}

	conn, err := net.DialTimeout("tcp", target.Addr, dialTimeout)
	if err != nil {
		return
	}
	defer conn.Close()

	_ = protocol.Send(conn, protocol.MsgGossip, protocol.GossipPayload{Peers: peerList})
}

func (n *Node) mergePeers(peers []protocol.PeerInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range peers {
		if p.ID == n.ID {
			continue // don't add ourselves
		}
		if existing, ok := n.peers[p.ID]; !ok || p.LastSeen.After(existing.LastSeen) {
			n.peers[p.ID] = p
		}
	}
}

// Peers returns a snapshot of known active peers.
func (n *Node) Peers() []protocol.PeerInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]protocol.PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, p)
	}
	return out
}

// RefreshPeers queries all discovery nodes for fresh peer lists.
func (n *Node) RefreshPeers() {
	for _, addr := range n.DiscoveryAddrs {
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err != nil {
			continue
		}
		_ = protocol.Send(conn, protocol.MsgGetPeers, struct{}{})
		env, err := protocol.Recv(conn, 10*time.Second)
		conn.Close()
		if err != nil || env.Type != protocol.MsgPeerList {
			continue
		}
		var pl protocol.PeerListPayload
		if protocol.Decode(env, &pl) == nil {
			n.mergePeers(pl.Peers)
		}
	}
}

// ListRemoteFiles fetches the file list from a specific peer.
func (n *Node) ListRemoteFiles(peerAddr string) ([]protocol.FileInfo, string, error) {
	conn, err := net.DialTimeout("tcp", peerAddr, dialTimeout)
	if err != nil {
		return nil, "", fmt.Errorf("peer unreachable (%s): %w", peerAddr, err)
	}
	defer conn.Close()

	if err := protocol.Send(conn, protocol.MsgListFiles, struct{}{}); err != nil {
		return nil, "", fmt.Errorf("send list request: %w", err)
	}
	env, err := protocol.Recv(conn, 10*time.Second)
	if err != nil {
		return nil, "", fmt.Errorf("recv file list: %w", err)
	}
	if env.Type == protocol.MsgError {
		var e protocol.ErrorPayload
		protocol.Decode(env, &e)
		return nil, "", fmt.Errorf("remote error: %s", e.Message)
	}
	if env.Type != protocol.MsgFileList {
		return nil, "", fmt.Errorf("unexpected response: %s", env.Type)
	}
	var fl protocol.FileListPayload
	if err := protocol.Decode(env, &fl); err != nil {
		return nil, "", err
	}
	return fl.Files, fl.PeerID, nil
}

// Download fetches a named file from peerAddr into DownloadDir.
// It verifies the MD5 checksum and removes the file if corrupted.
func (n *Node) Download(peerAddr, filename string) error {
	conn, err := net.DialTimeout("tcp", peerAddr, dialTimeout)
	if err != nil {
		return fmt.Errorf("peer unreachable (%s): %w", peerAddr, err)
	}
	defer conn.Close()

	if err := protocol.Send(conn, protocol.MsgDownload, protocol.DownloadPayload{Filename: filename}); err != nil {
		return fmt.Errorf("send download request: %w", err)
	}

	// Expect FILE_DATA metadata envelope
	env, err := protocol.Recv(conn, 10*time.Second)
	if err != nil {
		return fmt.Errorf("recv metadata: %w", err)
	}
	if env.Type == protocol.MsgError {
		var e protocol.ErrorPayload
		protocol.Decode(env, &e)
		return fmt.Errorf("remote error: %s", e.Message)
	}
	if env.Type != protocol.MsgFileData {
		return fmt.Errorf("unexpected response: %s", env.Type)
	}
	var meta protocol.FileDataPayload
	if err := protocol.Decode(env, &meta); err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}

	// Write to a temp file first to avoid incomplete files in download dir
	if err := os.MkdirAll(n.DownloadDir, 0755); err != nil {
		return fmt.Errorf("create download dir: %w", err)
	}

	tmpPath := filepath.Join(n.DownloadDir, meta.Filename+".tmp")
	finalPath := filepath.Join(n.DownloadDir, meta.Filename)

	// Remove leftover temp files
	os.Remove(tmpPath)

	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	// Receive raw bytes up to meta.Size, then read checksum line
	h := md5.New()
	buf := make([]byte, 32*1024)
	remaining := meta.Size
	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	for remaining > 0 {
		toRead := int64(len(buf))
		if toRead > remaining {
			toRead = remaining
		}
		n, err := conn.Read(buf[:toRead])
		if n > 0 {
			chunk := buf[:n]
			h.Write(chunk)
			if _, werr := out.Write(chunk); werr != nil {
				out.Close()
				os.Remove(tmpPath)
				return fmt.Errorf("write error: %w", werr)
			}
			remaining -= int64(n)
		}
		if err != nil {
			if err == io.EOF && remaining == 0 {
				break
			}
			out.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("read error (remaining=%d): %w", remaining, err)
		}
	}
	out.Close()

	localChecksum := hex.EncodeToString(h.Sum(nil))

	// Read the trailing checksum sent by the server
	// The server sends "\n<checksum>\n" after the bytes
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	remoteChecksum := ""
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			remoteChecksum = line
			break
		}
	}

	if remoteChecksum != "" && remoteChecksum != localChecksum {
		os.Remove(tmpPath)
		return fmt.Errorf("checksum mismatch: got %s want %s (file removed)", localChecksum, remoteChecksum)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp file: %w", err)
	}

	log.Printf("[peer] downloaded %s (%d bytes, md5=%s)", meta.Filename, meta.Size, localChecksum)
	return nil
}
