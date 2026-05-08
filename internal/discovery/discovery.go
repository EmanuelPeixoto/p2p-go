// Package discovery implements the distributed peer registry.
// Any peer can act as a discovery node. There is no single central server;
// multiple discovery nodes can coexist, making the system resilient to failures.
package discovery

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"p2p/internal/protocol"
)

const (
	peerTTL       = 60 * time.Second // forget peers not seen for this long
	cleanupPeriod = 15 * time.Second
)

// Registry keeps track of known peers and serves discovery requests.
type Registry struct {
	mu    sync.RWMutex
	peers map[string]protocol.PeerInfo // key = peer ID
}

func NewRegistry() *Registry {
	r := &Registry{peers: make(map[string]protocol.PeerInfo)}
	go r.cleanup()
	return r
}

// Register adds or refreshes a peer entry.
func (r *Registry) Register(info protocol.PeerInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info.LastSeen = time.Now()
	r.peers[info.ID] = info
}

// List returns all peers considered active.
func (r *Registry) List() []protocol.PeerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]protocol.PeerInfo, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

// Merge integrates gossip peers into the registry (used for resilience).
func (r *Registry) Merge(peers []protocol.PeerInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range peers {
		if existing, ok := r.peers[p.ID]; !ok || p.LastSeen.After(existing.LastSeen) {
			r.peers[p.ID] = p
		}
	}
}

func (r *Registry) cleanup() {
	for range time.Tick(cleanupPeriod) {
		r.mu.Lock()
		for id, p := range r.peers {
			if time.Since(p.LastSeen) > peerTTL {
				delete(r.peers, id)
				log.Printf("[discovery] removed stale peer %s", id)
			}
		}
		r.mu.Unlock()
	}
}

// Serve starts listening for discovery protocol connections on addr.
func (r *Registry) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("discovery listen %s: %w", addr, err)
	}
	log.Printf("[discovery] listening on %s", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[discovery] accept error: %v", err)
			continue
		}
		go r.handle(conn)
	}
}

func (r *Registry) handle(conn net.Conn) {
	defer conn.Close()
	for {
		env, err := protocol.Recv(conn, 30*time.Second)
		if err != nil {
			return // client disconnected or timed out
		}
		switch env.Type {
		case protocol.MsgRegister:
			var p protocol.RegisterPayload
			if err := protocol.Decode(env, &p); err != nil {
				sendError(conn, "bad register payload")
				return
			}
			r.Register(protocol.PeerInfo{ID: p.ID, Addr: p.Addr})
			log.Printf("[discovery] registered peer %s @ %s", p.ID, p.Addr)
			// reply with current peer list
			_ = protocol.Send(conn, protocol.MsgPeerList, protocol.PeerListPayload{Peers: r.List()})

		case protocol.MsgGetPeers:
			_ = protocol.Send(conn, protocol.MsgPeerList, protocol.PeerListPayload{Peers: r.List()})

		case protocol.MsgGossip:
			var g protocol.GossipPayload
			if err := protocol.Decode(env, &g); err == nil {
				r.Merge(g.Peers)
			}

		case protocol.MsgPing:
			_ = protocol.Send(conn, protocol.MsgPong, struct{}{})

		default:
			sendError(conn, "unknown message type: "+env.Type)
		}
	}
}

func sendError(conn net.Conn, msg string) {
	_ = protocol.Send(conn, protocol.MsgError, protocol.ErrorPayload{Message: msg})
}

// ServeListener accepts on an already-bound listener (used when binding before goroutine).
func (r *Registry) ServeListener(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[discovery] accept error: %v", err)
			continue
		}
		go r.handle(conn)
	}
}
