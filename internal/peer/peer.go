// Package peer implementa a lógica principal do nó P2P:
// registro na rede, descoberta de peers, gossip e download de arquivos.
// Downloads são retomáveis: o estado é salvo em .part + .state,
// e arquivos parciais ficam disponíveis para outros peers baixarem.
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

	"p2p/internal/fileserver"
	"p2p/internal/protocol"
)

const (
	heartbeatInterval = 20 * time.Second
	gossipInterval    = 30 * time.Second
	dialTimeout       = 5 * time.Second
)

// Node é um peer na rede P2P.
type Node struct {
	ID             string
	Addr           string   // host:porta do fileserver próprio
	DownloadDir    string
	DiscoveryAddrs []string

	mu    sync.RWMutex
	peers map[string]protocol.PeerInfo
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

// Start registra nos discovery nodes e inicia as goroutines de fundo.
func (n *Node) Start() {
	n.registerWithAll()
	go n.heartbeatLoop()
	go n.gossipLoop()
}

func (n *Node) registerWithAll() {
	for _, addr := range n.DiscoveryAddrs {
		peers, err := n.registerWith(addr)
		if err != nil {
			log.Printf("[peer] discovery %s indisponível: %v", addr, err)
			continue
		}
		n.mergePeers(peers)
		log.Printf("[peer] registrado em %s, %d peers conhecidos", addr, len(peers))
	}
}

func (n *Node) registerWith(discoveryAddr string) ([]protocol.PeerInfo, error) {
	conn, err := net.DialTimeout("tcp", discoveryAddr, dialTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := protocol.Send(conn, protocol.MsgRegister, protocol.RegisterPayload{
		ID: n.ID, Addr: n.Addr,
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
		return nil, fmt.Errorf("resposta inesperada: %s", env.Type)
	}
	var pl protocol.PeerListPayload
	if err := protocol.Decode(env, &pl); err != nil {
		return nil, err
	}
	return pl.Peers, nil
}

func (n *Node) heartbeatLoop() {
	for range time.Tick(heartbeatInterval) {
		n.registerWithAll()
	}
}

// gossipLoop compartilha peers conhecidos com um nó aleatório a cada 30s.
// Garante resiliência: se o discovery cair, peers continuam se encontrando.
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
			continue
		}
		if existing, ok := n.peers[p.ID]; !ok || p.LastSeen.After(existing.LastSeen) {
			n.peers[p.ID] = p
		}
	}
}

func (n *Node) Peers() []protocol.PeerInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]protocol.PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, p)
	}
	return out
}

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

func (n *Node) ListRemoteFiles(peerAddr string) ([]protocol.FileInfo, string, error) {
	conn, err := net.DialTimeout("tcp", peerAddr, dialTimeout)
	if err != nil {
		return nil, "", fmt.Errorf("peer indisponível (%s): %w", peerAddr, err)
	}
	defer conn.Close()
	if err := protocol.Send(conn, protocol.MsgListFiles, struct{}{}); err != nil {
		return nil, "", fmt.Errorf("envio falhou: %w", err)
	}
	env, err := protocol.Recv(conn, 10*time.Second)
	if err != nil {
		return nil, "", fmt.Errorf("sem resposta: %w", err)
	}
	if env.Type == protocol.MsgError {
		var e protocol.ErrorPayload
		protocol.Decode(env, &e)
		return nil, "", fmt.Errorf("erro remoto: %s", e.Message)
	}
	if env.Type != protocol.MsgFileList {
		return nil, "", fmt.Errorf("resposta inesperada: %s", env.Type)
	}
	var fl protocol.FileListPayload
	if err := protocol.Decode(env, &fl); err != nil {
		return nil, "", err
	}
	return fl.Files, fl.PeerID, nil
}

// Download baixa (ou retoma) um arquivo de peerAddr.
// Salva progresso em <arquivo>.part + <arquivo>.state no DownloadDir.
// Exibe porcentagem em tempo real no terminal.
// Arquivo parcial já disponível é compartilhado imediatamente.
func (n *Node) Download(peerAddr, filename string, progressCh chan<- float64) error {
	clean := filepath.Base(filename)
	partPath  := filepath.Join(n.DownloadDir, clean+".part")
	statePath := filepath.Join(n.DownloadDir, clean+".state")
	finalPath := filepath.Join(n.DownloadDir, clean)

	// Se já existe arquivo completo, não baixa de novo
	if _, err := os.Stat(finalPath); err == nil {
		return fmt.Errorf("arquivo já existe: %s", finalPath)
	}

	// Verifica quanto já foi baixado (retomada)
	var offset int64
	if info, err := os.Stat(partPath); err == nil {
		offset = info.Size()
		log.Printf("[peer] retomando %s a partir do byte %d", clean, offset)
	}

	// Conecta e pede o trecho a partir do offset
	conn, err := net.DialTimeout("tcp", peerAddr, dialTimeout)
	if err != nil {
		return fmt.Errorf("peer indisponível (%s): %w", peerAddr, err)
	}
	defer conn.Close()

	if err := protocol.Send(conn, protocol.MsgDownload, protocol.DownloadPayload{
		Filename: clean,
		Offset:   offset,
		Length:   0, // 0 = até o fim
	}); err != nil {
		return fmt.Errorf("envio falhou: %w", err)
	}

	// Recebe metadados
	env, err := protocol.Recv(conn, 10*time.Second)
	if err != nil {
		return fmt.Errorf("sem resposta do peer: %w", err)
	}
	if env.Type == protocol.MsgError {
		var e protocol.ErrorPayload
		protocol.Decode(env, &e)
		return fmt.Errorf("erro remoto: %s", e.Message)
	}
	if env.Type != protocol.MsgFileData {
		return fmt.Errorf("resposta inesperada: %s", env.Type)
	}
	var meta protocol.FileDataPayload
	if err := protocol.Decode(env, &meta); err != nil {
		return fmt.Errorf("metadados inválidos: %w", err)
	}

	if err := os.MkdirAll(n.DownloadDir, 0755); err != nil {
		return fmt.Errorf("criar diretório: %w", err)
	}

	// Salva estado inicial para que o fileserver possa servir o .part
	if err := fileserver.SaveState(statePath, meta.Size, offset); err != nil {
		log.Printf("[peer] aviso: não salvou estado: %v", err)
	}

	// Abre .part em modo append (retomada) ou cria do zero
	var out *os.File
	if offset > 0 {
		out, err = os.OpenFile(partPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		out, err = os.Create(partPath)
	}
	if err != nil {
		return fmt.Errorf("criar arquivo parcial: %w", err)
	}

	// Recebe bytes com progresso em tempo real
	h := md5.New()
	buf := make([]byte, 32*1024)
	remaining := meta.ChunkSize
	received  := offset
	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	for remaining > 0 {
		toRead := int64(len(buf))
		if toRead > remaining {
			toRead = remaining
		}
		nr, rerr := conn.Read(buf[:toRead])
		if nr > 0 {
			chunk := buf[:nr]
			h.Write(chunk)
			if _, werr := out.Write(chunk); werr != nil {
				out.Close()
				return fmt.Errorf("erro ao gravar: %w", werr)
			}
			remaining -= int64(nr)
			received  += int64(nr)

			// Atualiza estado em disco periodicamente (a cada 256 KB)
			if received%( 256*1024) == 0 {
				fileserver.SaveState(statePath, meta.Size, received)
			}

			// Envia progresso para o canal (não bloqueia)
			if progressCh != nil && meta.Size > 0 {
				pct := float64(received) / float64(meta.Size) * 100
				select {
				case progressCh <- pct:
				default:
				}
			}
		}
		if rerr != nil {
			if rerr == io.EOF && remaining == 0 {
				break
			}
			out.Close()
			// Salva progresso antes de retornar o erro
			fileserver.SaveState(statePath, meta.Size, received)
			return fmt.Errorf("conexão interrompida (recebido %d/%d bytes): %w",
				received, meta.Size, rerr)
		}
	}
	out.Close()

	// Salva estado final
	fileserver.SaveState(statePath, meta.Size, received)

	// Verifica checksum do trecho recebido
	localChecksum := hex.EncodeToString(h.Sum(nil))
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
		// Checksum errado: apaga o .part e começa do zero na próxima
		os.Remove(partPath)
		os.Remove(statePath)
		return fmt.Errorf("checksum inválido — arquivo corrompido, removido (tente novamente)")
	}

	// Arquivo completo: renomeia .part → nome final
	if received >= meta.Size {
		if err := os.Rename(partPath, finalPath); err != nil {
			return fmt.Errorf("renomear arquivo: %w", err)
		}
		os.Remove(statePath)
		log.Printf("[peer] download completo: %s (%d bytes)", clean, received)
	} else {
		log.Printf("[peer] download parcial salvo: %s (%d/%d bytes)", clean, received, meta.Size)
	}

	return nil
}
