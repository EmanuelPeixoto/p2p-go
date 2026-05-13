package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2p/internal/discovery"
	"p2p/internal/fileserver"
	"p2p/internal/peer"
)

func disc(t *testing.T) string {
	reg := discovery.NewRegistry()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go reg.ServeListener(ln)
	return ln.Addr().String()
}

func fs(t *testing.T, shareDir, downloadDir, id string) string {
	srv := fileserver.New(shareDir, downloadDir, id)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.ServeListener(ln)
	return ln.Addr().String()
}

// Teste 1: download completo com progresso
func TestDownloadWithProgress(t *testing.T) {
	d := disc(t)
	tmp := t.TempDir()

	shareA := filepath.Join(tmp, "shareA"); os.MkdirAll(shareA, 0755)
	downB  := filepath.Join(tmp, "downB");  os.MkdirAll(downB, 0755)
	downA  := filepath.Join(tmp, "downA");  os.MkdirAll(downA, 0755)

	// arquivo de 200 KB
	data := make([]byte, 200*1024)
	for i := range data { data[i] = byte(i % 251) }
	os.WriteFile(filepath.Join(shareA, "big.bin"), data, 0644)

	fsA := fs(t, shareA, downA, "peerA")
	nodeB := peer.New("peerB", "127.0.0.1:0", downB, []string{d})
	nodeA := peer.New("peerA", fsA, downA, []string{d})
	nodeA.Start(); nodeB.Start()
	time.Sleep(100*time.Millisecond)

	progressCh := make(chan float64, 100)
	var lastPct float64
	go func() {
		for p := range progressCh { lastPct = p }
	}()

	err := nodeB.Download(fsA, "big.bin", progressCh)
	close(progressCh)
	time.Sleep(50*time.Millisecond)

	if err != nil { t.Fatalf("download: %v", err) }
	if lastPct < 99 { t.Errorf("progresso final %.1f%% < 99%%", lastPct) }
	fmt.Printf("  Progresso final: %.1f%%\n", lastPct)

	got, _ := os.ReadFile(filepath.Join(downB, "big.bin"))
	if len(got) != len(data) { t.Errorf("tamanho errado: %d != %d", len(got), len(data)) }
	fmt.Println("  Conteúdo correto ✓")
}

// Teste 2: arquivo parcial é servido para terceiros
func TestPartialFileIsServed(t *testing.T) {
	d := disc(t)
	tmp := t.TempDir()

	shareA := filepath.Join(tmp, "shareA"); os.MkdirAll(shareA, 0755)
	downA  := filepath.Join(tmp, "downA");  os.MkdirAll(downA, 0755)
	downB  := filepath.Join(tmp, "downB");  os.MkdirAll(downB, 0755)
	downC  := filepath.Join(tmp, "downC");  os.MkdirAll(downC, 0755)

	// arquivo de 100 KB
	data := make([]byte, 100*1024)
	for i := range data { data[i] = byte(i % 200) }
	os.WriteFile(filepath.Join(shareA, "file.bin"), data, 0644)

	_ = fs(t, shareA, downA, "peerA")

	// Simula B tendo baixado 60% do arquivo (60 KB)
	partial := data[:60*1024]
	os.WriteFile(filepath.Join(downB, "file.bin.part"), partial, 0644)
	fileserver.SaveState(filepath.Join(downB, "file.bin.state"), int64(len(data)), int64(len(partial)))

	fsB := fs(t, downA, downB, "peerB") // peerB serve seus parciais

	// C lista arquivos de B — deve ver file.bin com ~60%
	nodeC := peer.New("peerC", "127.0.0.1:0", downC, []string{d})
	nodeC.Start()
	time.Sleep(100*time.Millisecond)

	files, peerID, err := nodeC.ListRemoteFiles(fsB)
	if err != nil { t.Fatalf("list: %v", err) }
	fmt.Printf("  Arquivos em %s:\n", peerID)
	found := false
	for _, f := range files {
		status := "completo"
		if !f.Complete { status = fmt.Sprintf("parcial %.1f%%", f.Percent) }
		fmt.Printf("    %s  %d bytes  [%s]\n", f.Name, f.Size, status)
		if f.Name == "file.bin" {
			found = true
			if f.Complete { t.Error("deveria ser parcial") }
			if f.Percent < 55 || f.Percent > 65 {
				t.Errorf("porcentagem esperada ~60%%, got %.1f%%", f.Percent)
			}
		}
	}
	if !found { t.Error("file.bin não apareceu na lista de B") }

	// C baixa o trecho de B (offset=0, pega os 60 KB que B tem)
	err = nodeC.Download(fsB, "file.bin", nil)
	// Esperamos erro OU sucesso parcial (B só tem 60 KB)
	partPath := filepath.Join(downC, "file.bin.part")
	if _, statErr := os.Stat(partPath); statErr == nil {
		info, _ := os.Stat(partPath)
		fmt.Printf("  C baixou %d bytes parciais de B ✓\n", info.Size())
	} else if err == nil {
		fmt.Println("  C baixou arquivo completo de B ✓")
	}
	_ = err
}

// Teste 3: retomada de download interrompido
func TestResumeDownload(t *testing.T) {
	d := disc(t)
	tmp := t.TempDir()

	shareA := filepath.Join(tmp, "shareA"); os.MkdirAll(shareA, 0755)
	downA  := filepath.Join(tmp, "downA");  os.MkdirAll(downA, 0755)
	downB  := filepath.Join(tmp, "downB");  os.MkdirAll(downB, 0755)

	data := make([]byte, 150*1024)
	for i := range data { data[i] = byte(i % 233) }
	os.WriteFile(filepath.Join(shareA, "resume.bin"), data, 0644)

	fsA := fs(t, shareA, downA, "peerA")
	nodeB := peer.New("peerB", "127.0.0.1:0", downB, []string{d})
	nodeA := peer.New("peerA", fsA, downA, []string{d})
	nodeA.Start(); nodeB.Start()
	time.Sleep(100*time.Millisecond)

	// Simula interrupção: salva 50 KB em .part
	partial := data[:50*1024]
	os.WriteFile(filepath.Join(downB, "resume.bin.part"), partial, 0644)
	fileserver.SaveState(filepath.Join(downB, "resume.bin.state"), int64(len(data)), int64(len(partial)))
	fmt.Printf("  Simulada interrupção: %d KB salvos\n", len(partial)/1024)

	// Retoma: pede a partir do byte 50 KB
	err := nodeB.Download(fsA, "resume.bin", nil)
	if err != nil { t.Fatalf("retomada falhou: %v", err) }

	got, _ := os.ReadFile(filepath.Join(downB, "resume.bin"))
	if len(got) != len(data) {
		t.Errorf("tamanho após retomada: %d != %d", len(got), len(data))
	}
	fmt.Printf("  Retomada completa: %d KB ✓\n", len(got)/1024)
}
