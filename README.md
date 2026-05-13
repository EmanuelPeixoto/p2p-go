# Sistema P2P de Compartilhamento de Arquivos

Sistema de compartilhamento de arquivos P2P em Go onde cada nó atua
simultaneamente como cliente e servidor. Downloads são **retomáveis**:
arquivos parcialmente baixados ficam disponíveis para outros peers
imediatamente, com indicação de porcentagem.

## Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│  Cada peer roda 3 componentes no mesmo processo:        │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  Discovery   │  │  FileServer  │  │  Peer Node   │   │
│  │  (porta N)   │  │  (porta N+1) │  │  (cliente)   │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Protocolo**: TCP com mensagens JSON delimitadas por `\n`.

**Resiliência**: múltiplos discovery nodes + gossip entre peers a cada 30s.

## Downloads parciais e retomada

Quando um download começa, o sistema cria dois arquivos:
- `arquivo.bin.part` — bytes já recebidos (serve imediatamente para outros peers)
- `arquivo.bin.state` — tamanho total e bytes recebidos

Se a conexão cair, o próximo `get` retoma de onde parou (range request).
Quando chega ao 100%, renomeia `.part` → `arquivo.bin` e apaga o `.state`.

Outros peers que executarem `ls` verão o arquivo com a porcentagem disponível:
```
  2   video.mp4    1.2 GB   192.168.1.10:6001  (parcial 43.2%)
```

## Estrutura do Projeto

```
p2p/
├── cmd/peer/main.go              # CLI e ponto de entrada
├── internal/
│   ├── protocol/protocol.go     # Tipos de mensagens e encoding TCP+JSON
│   ├── discovery/discovery.go   # Registro distribuído de peers
│   ├── fileserver/fileserver.go # Serve arquivos completos e parciais (.part)
│   └── peer/peer.go             # Lógica P2P: registro, gossip, download
├── load_test.go                 # Teste de carga (50 downloads simultâneos)
├── partial_test.go              # Testes de parcial, progresso e retomada
├── Makefile
└── go.mod
```

## Como Compilar

```bash
# Requer Go 1.21+
make build
# Binário: build/p2p-peer
```

## Como Usar

### Portas por nó

| Porta     | Serviço                                     |
|-----------|---------------------------------------------|
| `-port N` | Discovery (registro de peers)               |
| `N+1`     | FileServer (lista e download de arquivos)   |

### Exemplo: duas máquinas na rede

**Máquina A (192.168.1.10) — bootstrap:**
```bash
./p2p-peer -id maquinaA -port 6000 \
           -public-addr 192.168.1.10 \
           -share ./compartilhados \
           -download ./baixados
```

**Máquina B:**
```bash
./p2p-peer -id maquinaB -port 6000 \
           -public-addr 192.168.1.20 \
           -discovery "192.168.1.10:6000" \
           -share ./compartilhados \
           -download ./baixados
```

### Firewall (NixOS)

```nix
networking.firewall.allowedTCPPorts = [ 6000 6001 ];
```

## Comandos do CLI

| Comando               | Descrição                                           |
|-----------------------|-----------------------------------------------------|
| `ls`                  | Listar todos os arquivos da rede (com % se parcial) |
| `get <N ou nome>`     | Baixar pelo número ou nome, com barra de progresso  |
| `peers`               | Ver peers ativos                                    |
| `refresh`             | Atualizar lista de peers                            |
| `myfiles`             | Meus arquivos compartilhados                        |
| `quit`                | Sair                                                |

### Sessão de exemplo

```
[maquinaB]> ls
  N°    Arquivo          Tamanho    Fontes
  ──────────────────────────────────────────────────────────
  1     relatorio.pdf    2.3 MB     192.168.1.10:6001
  2     video.mp4        1.2 GB     192.168.1.10:6001  (parcial 43.2%)
  3     foto.jpg         820 KB     192.168.1.10:6001 (2 cópias)

[maquinaB]> get 2
  Baixando de 192.168.1.10:6001
  [████████████░░░░░░░░░░░░░░░░░░]  40.1%
```

Se a conexão cair durante o download, basta rodar `get 2` novamente
que o sistema retoma de onde parou.

## Flags

| Flag              | Padrão        | Descrição                                     |
|-------------------|---------------|-----------------------------------------------|
| `-id`             | hostname-port | Identificador único deste peer                |
| `-port`           | 6000          | Porta base (discovery=N, fileserver=N+1)      |
| `-public-addr`    | auto          | IP/hostname para anunciar a outros peers      |
| `-share`          | ./shared      | Pasta de arquivos completos para compartilhar |
| `-download`       | ./downloads   | Pasta de downloads (inclui os .part)          |
| `-discovery`      | (vazio)       | Discovery nodes externos (host:port,...)      |
| `-discovery-node` | false         | Forçar modo discovery                         |

## Testes

```bash
go test -v ./...
```

| Teste                    | O que verifica                                  |
|--------------------------|-------------------------------------------------|
| `TestConcurrentDownloads`| 50 downloads simultâneos de 512 KB              |
| `TestConcurrentPeers`    | 100 peers registrando ao mesmo tempo            |
| `TestDownloadWithProgress`| progresso 0→100% reportado corretamente        |
| `TestPartialFileIsServed` | .part aparece no ls com % correta e é servido  |
| `TestResumeDownload`      | retomada do byte exato após interrupção        |

## Protocolo de mensagens

```
REGISTER   peer → discovery   {"id":"A","addr":"192.168.1.10:6001"}
PEER_LIST  discovery → peer   {"peers":[...]}
LIST_FILES peer → peer        {}
FILE_LIST  peer → peer        {"peer_id":"A","files":[{"name":"x","size":1024,"have":512,"complete":false,"percent":50}]}
DOWNLOAD   peer → peer        {"filename":"x","offset":512,"length":0}
FILE_DATA  peer → peer        {"filename":"x","size":1024,"offset":512,"chunk_size":512}
[bytes brutos do trecho]
[MD5 hex do trecho + \n]
```

## Requisitos atendidos

| # | Requisito                           | Como                                              |
|---|-------------------------------------|---------------------------------------------------|
| 1 | Registro com ID, endereço, porta    | `MsgRegister` no discovery                        |
| 2 | Discovery não é servidor central    | Qualquer peer pode rodar o discovery              |
| 3 | Resiliente a quedas de discovery    | Múltiplos nodes + gossip entre peers              |
| 4 | Lista de peers ativos               | `peers` / `refresh` com TTL de 60s               |
| 5 | Lista de arquivos por peer          | `MsgListFiles` direto entre peers                 |
| 6 | Download direto sem discovery       | TCP direto no fileserver do peer alvo             |
| 7 | Protocolo de rede próprio           | TCP + JSON (sem biblioteca P2P)                   |
| 8 | Funciona em múltiplas máquinas      | Flag `-public-addr` + auto-detecção de IP         |
| 9 | Pasta específica por máquina        | Flag `-share`                                     |
| + | Downloads parciais compartilhados   | `.part` servido com offset; `ls` mostra %         |
| + | Retomada de download                | Range request a partir do offset salvo em .state  |
| + | Progresso em tempo real             | Barra `█░` atualizada por `\r` durante o get      |
