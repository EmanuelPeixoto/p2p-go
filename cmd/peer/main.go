package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"p2p/internal/discovery"
	"p2p/internal/fileserver"
	"p2p/internal/peer"
	"p2p/internal/protocol"
)

func main() {
	id          := flag.String("id", "", "Identificador único do peer")
	port        := flag.Int("port", 6000, "Porta base. Discovery=porta, FileServer=porta+1")
	publicAddr  := flag.String("public-addr", "", "IP público desta máquina")
	shareDir    := flag.String("share", "./shared", "Diretório de arquivos para compartilhar")
	downloadDir := flag.String("download", "./downloads", "Diretório de downloads")
	discoveryFlag := flag.String("discovery", "", "Discovery nodes (ex: 192.168.1.10:6000)")
	asDiscovery := flag.Bool("discovery-node", false, "Rodar serviço de descoberta aqui")
	flag.Parse()

	if *id == "" {
		hostname, _ := os.Hostname()
		*id = hostname + "-" + strconv.Itoa(*port)
	}
	if *publicAddr == "" {
		*publicAddr = detectLocalIP()
	}
	for _, dir := range []string{*shareDir, *downloadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("não foi possível criar diretório %s: %v", dir, err)
		}
	}

	discPort      := *port
	filePort      := *port + 1
	discAddr      := fmt.Sprintf("0.0.0.0:%d", discPort)
	fileAddr      := fmt.Sprintf("0.0.0.0:%d", filePort)
	advertiseAddr := fmt.Sprintf("%s:%d", *publicAddr, filePort)

	var discoveryAddrs []string
	if *discoveryFlag != "" {
		for _, a := range strings.Split(*discoveryFlag, ",") {
			if a = strings.TrimSpace(a); a != "" {
				discoveryAddrs = append(discoveryAddrs, a)
			}
		}
	}
	isDiscovery := *asDiscovery || len(discoveryAddrs) == 0

	// Redireciona todos os logs para arquivo — não atrapalha o menu interativo.
	// Use "tail -f peer-<id>.log" em outro terminal para acompanhar.
	logPath := fmt.Sprintf("peer-%s.log", *id)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aviso: não foi possível criar log %s: %v\n", logPath, err)
	} else {
		log.SetOutput(logFile)
		log.SetFlags(log.Ltime)
	}

	// Resumo de startup vai para o terminal normalmente
	fmt.Printf("Iniciando  id=%-15s  endereço=%-22s  share=%s\n", *id, advertiseAddr, *shareDir)
	fmt.Printf("Logs em: %s\n", logPath)

	if isDiscovery {
		reg := discovery.NewRegistry()
		ln, err := net.Listen("tcp", discAddr)
		if err != nil {
			log.Fatalf("discovery: %v", err)
		}
		log.Printf("[discovery] pronto em %s", discAddr)
		go func() { reg.ServeListener(ln) }()
		discoveryAddrs = append(discoveryAddrs, fmt.Sprintf("127.0.0.1:%d", discPort))
	}

	fs := fileserver.New(*shareDir, *id)
	fln, err := net.Listen("tcp", fileAddr)
	if err != nil {
		log.Fatalf("fileserver: %v", err)
	}
	log.Printf("[fileserver] pronto em %s", fileAddr)
	go func() { fs.ServeListener(fln) }()

	node := peer.New(*id, advertiseAddr, *downloadDir, discoveryAddrs)
	node.Start()

	// ── CLI ──────────────────────────────────────────────────────────────
	fmt.Printf("\n[ P2P File Sharing — %s @ %s ]\n", *id, advertiseAddr)
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[%s]> ", *id)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd   := parts[0]

		switch cmd {

		case "help", "?":
			printHelp()

		case "quit", "exit", "q":
			fmt.Println("Até mais!")
			os.Exit(0)

		// ── peers ──────────────────────────────────────────────────────
		case "peers":
			peers := node.Peers()
			if len(peers) == 0 {
				fmt.Println("  Nenhum peer conhecido. Tente 'refresh'.")
				break
			}
			fmt.Printf("  %-20s  %-25s  %s\n", "ID", "Endereço", "Visto há")
			fmt.Println("  " + strings.Repeat("─", 60))
			for _, p := range peers {
				fmt.Printf("  %-20s  %-25s  %s\n", p.ID, p.Addr,
					time.Since(p.LastSeen).Round(time.Second))
			}

		case "refresh":
			node.RefreshPeers()
			fmt.Println("  Atualizado.")

		// ── listar todos os arquivos da rede ───────────────────────────
		case "ls", "list", "all-files":
			catalog := buildCatalog(node)
			printCatalog(catalog)

		// ── meus arquivos ──────────────────────────────────────────────
		case "myfiles":
			entries, err := os.ReadDir(*shareDir)
			if err != nil {
				fmt.Printf("  Erro: %v\n", err)
				break
			}
			fmt.Printf("  Meus arquivos (%s):\n", *shareDir)
			count := 0
			for _, e := range entries {
				if !e.IsDir() {
					info, _ := e.Info()
					fmt.Printf("    %-40s  %s\n", e.Name(), formatSize(info.Size()))
					count++
				}
			}
			if count == 0 {
				fmt.Println("    (nenhum arquivo)")
			}

		// ── get <número ou nome> ───────────────────────────────────────
		// Sem precisar digitar IP: busca o arquivo em todos os peers,
		// tenta o mais rápido e faz fallback para os outros se falhar.
		case "get":
			if len(parts) < 2 {
				fmt.Println("  Uso: get <número da lista>  ou  get <nome-do-arquivo>")
				fmt.Println("  Dica: rode 'ls' primeiro para ver a lista numerada.")
				break
			}
			target := strings.Join(parts[1:], " ")
			catalog := buildCatalog(node)

			// aceita número da lista
			entry := resolveTarget(catalog, target)
			if entry == nil {
				fmt.Printf("  Arquivo não encontrado na rede: %s\n", target)
				break
			}

			if len(entry.Sources) == 1 {
				doDownload(node, entry.Name, entry.Sources, *downloadDir)
			} else {
				// arquivo em mais de uma fonte — baixa do mais rápido
				fmt.Printf("  Arquivo disponível em %d peers. Testando o mais rápido...\n",
					len(entry.Sources))
				doDownload(node, entry.Name, entry.Sources, *downloadDir)
			}

		// ── compat: get <addr> <arquivo> (forma original) ─────────────
		// detecta se o primeiro argumento parece um endereço
		default:
			fmt.Printf("  Comando desconhecido: %q  (help para ajuda)\n", cmd)
		}
	}
}

// ── catálogo: mapeia nome de arquivo → fontes ────────────────────────────

type FileEntry struct {
	Name    string
	Size    int64
	Sources []string // endereços dos peers que têm o arquivo
}

// buildCatalog pergunta a todos os peers da rede quais arquivos têm.
// Consultas são feitas em paralelo para não travar o terminal.
func buildCatalog(node *peer.Node) []FileEntry {
	peers := node.Peers()
	if len(peers) == 0 {
		return nil
	}

	type result struct {
		addr  string
		files []protocol.FileInfo
	}
	ch := make(chan result, len(peers))

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			files, _, err := node.ListRemoteFiles(addr)
			if err != nil {
				ch <- result{addr: addr, files: nil}
				return
			}
			ch <- result{addr: addr, files: files}
		}(p.Addr)
	}
	wg.Wait()
	close(ch)

	// agrupa por nome de arquivo
	index := map[string]*FileEntry{}
	for r := range ch {
		for _, f := range r.files {
			e, ok := index[f.Name]
			if !ok {
				e = &FileEntry{Name: f.Name, Size: f.Size}
				index[f.Name] = e
			}
			e.Sources = append(e.Sources, r.addr)
		}
	}

	// ordena por nome para lista estável
	catalog := make([]FileEntry, 0, len(index))
	for _, e := range index {
		catalog = append(catalog, *e)
	}
	sort.Slice(catalog, func(i, j int) bool {
		return catalog[i].Name < catalog[j].Name
	})
	return catalog
}

func printCatalog(catalog []FileEntry) {
	if len(catalog) == 0 {
		fmt.Println("  Nenhum arquivo disponível na rede.")
		return
	}
	fmt.Printf("  %-4s  %-42s  %-10s  %s\n", "N°", "Arquivo", "Tamanho", "Fontes")
	fmt.Println("  " + strings.Repeat("─", 72))
	for i, e := range catalog {
		tag := ""
		if len(e.Sources) > 1 {
			tag = fmt.Sprintf(" (%d cópias)", len(e.Sources))
		}
		fmt.Printf("  %-4d  %-42s  %-10s  %s%s\n",
			i+1, e.Name, formatSize(e.Size), e.Sources[0], tag)
	}
}

// resolveTarget aceita número ("3") ou nome ("foto.png")
func resolveTarget(catalog []FileEntry, target string) *FileEntry {
	// tenta como número
	if n, err := strconv.Atoi(target); err == nil && n >= 1 && n <= len(catalog) {
		e := catalog[n-1]
		return &e
	}
	// tenta como nome exato
	for _, e := range catalog {
		if e.Name == target {
			ec := e
			return &ec
		}
	}
	// tenta como substring
	for _, e := range catalog {
		if strings.Contains(strings.ToLower(e.Name), strings.ToLower(target)) {
			ec := e
			return &ec
		}
	}
	return nil
}

// doDownload tenta baixar de cada fonte em ordem de latência.
// Mede a latência de cada fonte em paralelo e usa a mais rápida primeiro.
func doDownload(node *peer.Node, filename string, sources []string, downloadDir string) {
	ordered := rankByLatency(sources)

	for i, src := range ordered {
		label := src
		if len(ordered) > 1 {
			label = fmt.Sprintf("%s (opção %d/%d)", src, i+1, len(ordered))
		}
		fmt.Printf("  Baixando de %s ...\n", label)
		start := time.Now()
		if err := node.Download(src, filename); err != nil {
			fmt.Printf("  ✗ Falhou: %v\n", err)
			if i+1 < len(ordered) {
				fmt.Println("  Tentando próxima fonte...")
			}
			continue
		}
		fmt.Printf("  ✓ Concluído em %s  →  %s/%s\n",
			time.Since(start).Round(time.Millisecond), downloadDir, filename)
		return
	}
	fmt.Println("  Erro: nenhuma fonte disponível.")
}

// rankByLatency mede o tempo de conexão TCP a cada fonte em paralelo
// e retorna a lista ordenada da mais rápida para a mais lenta.
func rankByLatency(sources []string) []string {
	if len(sources) == 1 {
		return sources
	}

	type measured struct {
		addr    string
		latency time.Duration
	}
	ch := make(chan measured, len(sources))

	for _, src := range sources {
		go func(addr string) {
			start := time.Now()
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			lat := time.Since(start)
			if err != nil {
				lat = 999 * time.Second // penaliza inacessíveis
			} else {
				conn.Close()
			}
			ch <- measured{addr, lat}
		}(src)
	}

	results := make([]measured, 0, len(sources))
	for range sources {
		results = append(results, <-ch)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].latency < results[j].latency
	})

	out := make([]string, len(results))
	for i, r := range results {
		out[i] = r.addr
	}
	return out
}

// ── helpers ──────────────────────────────────────────────────────────────

func printHelp() {
	fmt.Println("  ls              — listar todos os arquivos da rede (numerados)")
	fmt.Println("  get <N ou nome> — baixar arquivo pelo número ou nome")
	fmt.Println("  peers           — ver peers ativos")
	fmt.Println("  refresh         — atualizar lista de peers")
	fmt.Println("  myfiles         — meus arquivos compartilhados")
	fmt.Println("  quit            — sair")
	fmt.Println()
}

func formatSize(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func detectLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
