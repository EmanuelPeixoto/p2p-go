// Package fileserver handles the file-sharing side of each peer:
// listing shared files and serving downloads directly to other peers.
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
	"time"

	"p2p/internal/protocol"
)

// Server listens for file requests from other peers.
type Server struct {
	ShareDir string
	PeerID   string
}

func New(shareDir, peerID string) *Server {
	return &Server{ShareDir: shareDir, PeerID: peerID}
}

// Serve starts the file server on addr.
func (s *Server) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("fileserver listen %s: %w", addr, err)
	}
	log.Printf("[fileserver] listening on %s  share=%s", addr, s.ShareDir)
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
		s.sendFile(conn, req.Filename)

	default:
		sendError(conn, "unexpected message: "+env.Type)
	}
}

func (s *Server) listFiles() ([]protocol.FileInfo, error) {
	entries, err := os.ReadDir(s.ShareDir)
	if err != nil {
		return nil, err
	}
	var files []protocol.FileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, protocol.FileInfo{Name: e.Name(), Size: info.Size()})
	}
	return files, nil
}

func (s *Server) sendFile(conn net.Conn, filename string) {
	// Security: reject path traversal attempts
	clean := filepath.Base(filename)
	path := filepath.Join(s.ShareDir, clean)

	f, err := os.Open(path)
	if err != nil {
		sendError(conn, fmt.Sprintf("file not found: %s", clean))
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		sendError(conn, "cannot stat file")
		return
	}

	// Send metadata first
	if err := protocol.Send(conn, protocol.MsgFileData, protocol.FileDataPayload{
		Filename: clean,
		Size:     info.Size(),
	}); err != nil {
		return
	}

	// Stream raw bytes, compute checksum
	h := md5.New()
	buf := make([]byte, 32*1024)
	total := int64(0)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			h.Write(chunk)
			if _, werr := conn.Write(chunk); werr != nil {
				log.Printf("[fileserver] write error sending %s: %v", clean, werr)
				return
			}
			total += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[fileserver] read error for %s: %v", clean, err)
			return
		}
	}
	checksum := hex.EncodeToString(h.Sum(nil))
	log.Printf("[fileserver] sent %s (%d bytes, md5=%s)", clean, total, checksum)

	// Send checksum as a trailing newline-delimited line so client can verify
	conn.Write([]byte("\n" + checksum + "\n"))
}

func sendError(conn net.Conn, msg string) {
	_ = protocol.Send(conn, protocol.MsgError, protocol.ErrorPayload{Message: msg})
}

// ServeListener accepts on an already-bound listener.
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
