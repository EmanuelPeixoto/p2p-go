// Package protocol define todos os tipos de mensagem trocados entre peers.
// Todas as mensagens são JSON delimitado por newline (\n) sobre TCP.
package protocol

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// Tipos de mensagem
const (
	MsgRegister  = "REGISTER"   // peer → discovery: anunciar presença
	MsgPeerList  = "PEER_LIST"  // discovery → peer: lista de peers conhecidos
	MsgGetPeers  = "GET_PEERS"  // peer → discovery: pedir lista de peers
	MsgListFiles = "LIST_FILES" // peer → peer: pedir lista de arquivos
	MsgFileList  = "FILE_LIST"  // peer → peer: responder com lista de arquivos
	MsgDownload  = "DOWNLOAD"   // peer → peer: pedir trecho de arquivo
	MsgFileData  = "FILE_DATA"  // peer → peer: metadados antes dos bytes
	MsgError     = "ERROR"      // qualquer: resposta de erro
	MsgPing      = "PING"       // heartbeat
	MsgPong      = "PONG"       // resposta de heartbeat
	MsgGossip    = "GOSSIP"     // peer → peer: compartilhar peers conhecidos
)

// Envelope envolve toda mensagem para o receptor saber como decodificar.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// PeerInfo descreve um peer na rede.
type PeerInfo struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"` // host:porta
	LastSeen time.Time `json:"last_seen"`
}

// RegisterPayload é enviado pelo peer ao entrar na rede.
type RegisterPayload struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// PeerListPayload carrega a lista de peers ativos.
type PeerListPayload struct {
	Peers []PeerInfo `json:"peers"`
}

// FileInfo descreve um arquivo compartilhado — completo ou parcial.
type FileInfo struct {
	Name     string  `json:"name"`
	Size     int64   `json:"size"`               // tamanho total conhecido
	Have     int64   `json:"have"`               // bytes já recebidos (0 = completo local)
	Complete bool    `json:"complete"`           // true = arquivo completo
	Percent  float64 `json:"percent,omitempty"`  // 0–100, preenchido pelo receptor
}

// FileListPayload carrega a lista de arquivos de um peer.
type FileListPayload struct {
	PeerID string     `json:"peer_id"`
	Files  []FileInfo `json:"files"`
}

// DownloadPayload pede um trecho de arquivo a um peer.
// Offset=0 e Length=0 significa "arquivo inteiro".
type DownloadPayload struct {
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`  // byte de início
	Length   int64  `json:"length"`  // quantos bytes enviar (0 = até o fim)
}

// FileDataPayload precede os bytes brutos de uma transferência.
type FileDataPayload struct {
	Filename  string `json:"filename"`
	Size      int64  `json:"size"`       // tamanho total do arquivo
	Offset    int64  `json:"offset"`     // offset deste trecho
	ChunkSize int64  `json:"chunk_size"` // bytes que virão a seguir
}

// ErrorPayload carrega uma mensagem de erro legível.
type ErrorPayload struct {
	Message string `json:"message"`
}

// GossipPayload carrega peers que o nó conhece (resiliência).
type GossipPayload struct {
	Peers []PeerInfo `json:"peers"`
}

// --- Helpers ---

// Send codifica e envia um envelope pela conexão.
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

// Recv lê um envelope JSON delimitado por newline da conexão.
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
		if len(buf) > 10*1024*1024 {
			return nil, fmt.Errorf("envelope too large")
		}
	}
	var env Envelope
	if err := json.Unmarshal(buf, &env); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	return &env, nil
}

// Decode desempacota o payload de um envelope em dst.
func Decode(env *Envelope, dst any) error {
	return json.Unmarshal(env.Payload, dst)
}
