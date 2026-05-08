// Package protocol defines all message types and encoding used between peers.
// All messages are newline-delimited JSON over TCP.
package protocol

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// Message types
const (
	MsgRegister      = "REGISTER"       // peer → discovery: announce self
	MsgPeerList      = "PEER_LIST"      // discovery → peer: list of known peers
	MsgGetPeers      = "GET_PEERS"      // peer → discovery: request peer list
	MsgListFiles     = "LIST_FILES"     // peer → peer: request file list
	MsgFileList      = "FILE_LIST"      // peer → peer: respond with file list
	MsgDownload      = "DOWNLOAD"       // peer → peer: request file download
	MsgFileData      = "FILE_DATA"      // peer → peer: file metadata before sending bytes
	MsgError         = "ERROR"          // any: error response
	MsgPing          = "PING"           // heartbeat request
	MsgPong          = "PONG"           // heartbeat response
	MsgGossip        = "GOSSIP"         // peer → peer: share known peers (resilience)
)

// Envelope wraps every message so the receiver knows how to decode the payload.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// PeerInfo describes a single peer in the network.
type PeerInfo struct {
	ID        string    `json:"id"`
	Addr      string    `json:"addr"` // host:port
	LastSeen  time.Time `json:"last_seen"`
}

// RegisterPayload is sent by a peer when it joins.
type RegisterPayload struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// PeerListPayload carries the list of known active peers.
type PeerListPayload struct {
	Peers []PeerInfo `json:"peers"`
}

// FileInfo describes a single shared file.
type FileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// FileListPayload carries the list of shared files.
type FileListPayload struct {
	PeerID string     `json:"peer_id"`
	Files  []FileInfo `json:"files"`
}

// DownloadPayload requests a file from a peer.
type DownloadPayload struct {
	Filename string `json:"filename"`
}

// FileDataPayload precedes the raw bytes of a file transfer.
type FileDataPayload struct {
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

// ErrorPayload carries a human-readable error message.
type ErrorPayload struct {
	Message string `json:"message"`
}

// GossipPayload carries peers a node knows about (for resilience).
type GossipPayload struct {
	Peers []PeerInfo `json:"peers"`
}

// --- Helpers ---

// Send encodes and sends an envelope over conn.
func Send(conn net.Conn, msgType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	env := Envelope{Type: msgType, Payload: raw}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	data = append(data, '\n')
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.Write(data)
	return err
}

// Recv reads one newline-delimited JSON envelope from conn.
func Recv(conn net.Conn, timeout time.Duration) (*Envelope, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 1)
	for {
		_, err := conn.Read(tmp)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		if tmp[0] == '\n' {
			break
		}
		buf = append(buf, tmp[0])
		if len(buf) > 10*1024*1024 { // 10 MB envelope guard
			return nil, fmt.Errorf("envelope too large")
		}
	}
	var env Envelope
	if err := json.Unmarshal(buf, &env); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	return &env, nil
}

// Decode unmarshals the raw payload of an envelope into dst.
func Decode(env *Envelope, dst any) error {
	return json.Unmarshal(env.Payload, dst)
}
