// Package fileserver serve arquivos a outros peers — completos ou parciais.
// Arquivos em download (.part) também são compartilhados, com a porcentagem
// já recebida, para que outros peers possam baixar o que já existe.
package fileserver

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"p2p/internal/protocol"
)

// Server escuta requisições de arquivo de outros peers.
type Server struct {
	ShareDir   string // arquivos completos para compartilhar
	DownloadDir string // onde ficam os .part (downloads em andamento)
	PeerID     string
}

func New(shareDir, downloadDir, peerID string) *Server {
	return &Server{ShareDir: shareDir, DownloadDir: downloadDir, PeerID: peerID}
}

// ServeListener aceita conexões num listener já aberto.
func (s *Server) ServeListener(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[fileserver] accept error: %v", err)
			continue
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	env, err := protocol.Recv(conn, 15*time.Second)
	if err != nil {
		return
	}
	switch env.Type {
	case protocol.MsgListFiles:
		files, err := s.listFiles()
		if err != nil {
			sendError(conn, "cannot list files: "+err.Error())
			return
		}
		_ = protocol.Send(conn, protocol.MsgFileList, protocol.FileListPayload{
			PeerID: s.PeerID,
			Files:  files,
		})

	case protocol.MsgDownload:
		var req protocol.DownloadPayload
		if err := protocol.Decode(env, &req); err != nil {
			sendError(conn, "bad download request")
			return
		}
		s.sendFile(conn, req.Filename, req.Offset, req.Length)

	default:
		sendError(conn, "unexpected message: "+env.Type)
	}
}

// listFiles lista arquivos completos (ShareDir) e parciais (DownloadDir).
func (s *Server) listFiles() ([]protocol.FileInfo, error) {
	var files []protocol.FileInfo

	// 1. Arquivos completos na pasta de compartilhamento
	if err := addCompleteFiles(s.ShareDir, &files); err != nil {
		return nil, err
	}

	// 2. Arquivos parciais (.part) na pasta de downloads
	addPartialFiles(s.DownloadDir, &files)

	return files, nil
}

func addCompleteFiles(dir string, out *[]protocol.FileInfo) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".part") || strings.HasSuffix(e.Name(), ".state") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		*out = append(*out, protocol.FileInfo{
			Name:     e.Name(),
			Size:     info.Size(),
			Have:     info.Size(),
			Complete: true,
			Percent:  100,
		})
	}
	return nil
}

func addPartialFiles(dir string, out *[]protocol.FileInfo) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".part") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		// Lê o arquivo de estado para saber o tamanho total
		baseName := strings.TrimSuffix(e.Name(), ".part")
		state := loadState(filepath.Join(dir, baseName+".state"))
		if state == nil || state.TotalSize == 0 {
			continue // sem estado válido não sabemos o tamanho total
		}
		have := info.Size()
		pct := float64(have) / float64(state.TotalSize) * 100
		*out = append(*out, protocol.FileInfo{
			Name:     baseName,
			Size:     state.TotalSize,
			Have:     have,
			Complete: false,
			Percent:  pct,
		})
	}
}

// sendFile envia um trecho (ou o arquivo inteiro) ao peer solicitante.
// Procura primeiro na ShareDir (completo), depois na DownloadDir (.part).
func (s *Server) sendFile(conn net.Conn, filename string, offset, length int64) {
	clean := filepath.Base(filename)

	// Tenta arquivo completo primeiro
	path := filepath.Join(s.ShareDir, clean)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Tenta o .part em downloads
		partPath := filepath.Join(s.DownloadDir, clean+".part")
		if _, err2 := os.Stat(partPath); err2 != nil {
			sendError(conn, fmt.Sprintf("file not found: %s", clean))
			return
		}
		path = partPath
	}

	f, err := os.Open(path)
	if err != nil {
		sendError(conn, fmt.Sprintf("cannot open: %s", clean))
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		sendError(conn, "cannot stat file")
		return
	}

	// Determina o tamanho total real (pode ser .part de um arquivo maior)
	totalSize := info.Size()
	state := loadState(filepath.Join(s.DownloadDir, clean+".state"))
	if state != nil && state.TotalSize > 0 {
		totalSize = state.TotalSize
	}

	// Calcula trecho a enviar
	if offset < 0 || offset > info.Size() {
		sendError(conn, fmt.Sprintf("invalid offset %d (have %d bytes)", offset, info.Size()))
		return
	}
	available := info.Size() - offset
	chunkSize := available
	if length > 0 && length < chunkSize {
		chunkSize = length
	}

	// Envia metadados
	if err := protocol.Send(conn, protocol.MsgFileData, protocol.FileDataPayload{
		Filename:  clean,
		Size:      totalSize,
		Offset:    offset,
		ChunkSize: chunkSize,
	}); err != nil {
		return
	}

	// Posiciona no offset solicitado
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			log.Printf("[fileserver] seek error: %v", err)
			return
		}
	}

	// Envia os bytes + checksum MD5 do trecho
	h := md5.New()
	buf := make([]byte, 32*1024)
	remaining := chunkSize
	conn.SetWriteDeadline(time.Now().Add(5 * time.Minute))

	for remaining > 0 {
		toRead := int64(len(buf))
		if toRead > remaining {
			toRead = remaining
		}
		n, err := f.Read(buf[:toRead])
		if n > 0 {
			chunk := buf[:n]
			h.Write(chunk)
			if _, werr := conn.Write(chunk); werr != nil {
				log.Printf("[fileserver] write error: %v", werr)
				return
			}
			remaining -= int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
	}
	checksum := hex.EncodeToString(h.Sum(nil))
	log.Printf("[fileserver] sent %s offset=%d chunk=%d md5=%s", clean, offset, chunkSize, checksum)
	conn.Write([]byte("\n" + checksum + "\n"))
}

// DownloadState é persistido em disco (.state) para retomar downloads.
type DownloadState struct {
	TotalSize int64  `json:"total_size"`
	Received  int64  `json:"received"`
}

func loadState(path string) *DownloadState {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	// formato simples: "total received\n"
	var s DownloadState
	fmt.Sscanf(string(data), "%d %d", &s.TotalSize, &s.Received)
	if s.TotalSize == 0 {
		return nil
	}
	return &s
}

func SaveState(path string, totalSize, received int64) error {
	return os.WriteFile(path, []byte(fmt.Sprintf("%d %d\n", totalSize, received)), 0644)
}

func sendError(conn net.Conn, msg string) {
	_ = protocol.Send(conn, protocol.MsgError, protocol.ErrorPayload{Message: msg})
}
