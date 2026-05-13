package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"p2p/internal/discovery"
	"p2p/internal/fileserver"
	"p2p/internal/peer"
)

func startDiscovery(t *testing.T) string {
	reg := discovery.NewRegistry()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	go reg.ServeListener(ln)
	return ln.Addr().String()
}

func startFileServer(t *testing.T, dir, id string) string {
	fs := fileserver.New(dir, dir, id)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	go fs.ServeListener(ln)
	return ln.Addr().String()
}

// TestConcurrentDownloads simula N peers baixando o mesmo arquivo ao mesmo tempo.
func TestConcurrentDownloads(t *testing.T) {
	const numClients = 50

	disc := startDiscovery(t)
	tmp  := t.TempDir()

	// Servidor com um arquivo de 512 KB
	shareDir := filepath.Join(tmp, "server")
	os.MkdirAll(shareDir, 0755)
	data := make([]byte, 512*1024)
	for i := range data { data[i] = byte(i % 251) }
	os.WriteFile(filepath.Join(shareDir, "arquivo.bin"), data, 0644)

	fsAddr := startFileServer(t, shareDir, "servidor")
	srv := peer.New("servidor", fsAddr, tmp, []string{disc})
	srv.Start()
	time.Sleep(100 * time.Millisecond)

	var (
		wg      sync.WaitGroup
		ok      atomic.Int64
		failed  atomic.Int64
		totalMS atomic.Int64
	)

	fmt.Printf("\n  Testando %d downloads simultâneos de 512 KB...\n", numClients)
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			downDir := filepath.Join(tmp, fmt.Sprintf("client%d", n))
			os.MkdirAll(downDir, 0755)
			node := peer.New(fmt.Sprintf("client%d", n), "127.0.0.1:0", downDir, []string{disc})
			node.Start()

			t0 := time.Now()
			err := node.Download(fsAddr, "arquivo.bin", nil)
			totalMS.Add(time.Since(t0).Milliseconds())

			if err != nil {
				failed.Add(1)
				t.Logf("  cliente %d falhou: %v", n, err)
				return
			}
			info, _ := os.Stat(filepath.Join(downDir, "arquivo.bin"))
			if info == nil || info.Size() != int64(len(data)) {
				failed.Add(1)
				t.Logf("  cliente %d: tamanho errado", n)
				return
			}
			ok.Add(1)
		}(i)
	}

	wg.Wait()
	total := time.Since(start)

	n := ok.Load() + failed.Load()
	avgMS := time.Duration(0)
	if n > 0 { avgMS = time.Duration(totalMS.Load()/n) * time.Millisecond }

	fmt.Printf("\n  ┌─ Resultado ────────────────────────────────\n")
	fmt.Printf("  │  Clientes simultâneos : %d\n", numClients)
	fmt.Printf("  │  Sucessos             : %d\n", ok.Load())
	fmt.Printf("  │  Falhas               : %d\n", failed.Load())
	fmt.Printf("  │  Tempo total          : %s\n", total.Round(time.Millisecond))
	fmt.Printf("  │  Tempo médio/download : %s\n", avgMS)
	fmt.Printf("  └────────────────────────────────────────────\n\n")

	if failed.Load() > 0 {
		t.Errorf("%d downloads falharam", failed.Load())
	}
}

// TestConcurrentPeers testa registro simultâneo de muitos peers no discovery.
func TestConcurrentPeers(t *testing.T) {
	const numPeers = 100

	disc := startDiscovery(t)
	tmp  := t.TempDir()

	var wg sync.WaitGroup
	var registered atomic.Int64

	fmt.Printf("\n  Registrando %d peers simultaneamente...\n", numPeers)
	start := time.Now()

	for i := 0; i < numPeers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			downDir := filepath.Join(tmp, fmt.Sprintf("p%d", n))
			os.MkdirAll(downDir, 0755)
			node := peer.New(
				fmt.Sprintf("peer%03d", n),
				fmt.Sprintf("127.0.0.1:%d", 20000+n),
				downDir,
				[]string{disc},
			)
			node.Start()
			registered.Add(1)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	time.Sleep(100 * time.Millisecond)
	probe := peer.New("probe", "127.0.0.1:0", tmp, []string{disc})
	probe.Start()
	time.Sleep(50 * time.Millisecond)
	probe.RefreshPeers()
	knownPeers := probe.Peers()

	// ordena para exibição legível
	sort.Slice(knownPeers, func(i, j int) bool {
		return knownPeers[i].ID < knownPeers[j].ID
	})

	fmt.Printf("\n  ┌─ Resultado ────────────────────────────────\n")
	fmt.Printf("  │  Peers lançados       : %d\n", numPeers)
	fmt.Printf("  │  Registros concluídos : %d\n", registered.Load())
	fmt.Printf("  │  Visíveis no discovery: %d\n", len(knownPeers))
	fmt.Printf("  │  Tempo total          : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  └────────────────────────────────────────────\n\n")

	if int(registered.Load()) != numPeers {
		t.Errorf("esperava %d registros, got %d", numPeers, registered.Load())
	}
}
