# Sistema P2P de Compartilhamento de Arquivos

Sistema simplificado de compartilhamento de arquivos P2P implementado em Go,
onde cada nó atua simultaneamente como cliente e servidor.

## Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│  Cada peer roda 3 componentes no mesmo processo:        │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Discovery   │  │  FileServer  │  │  Peer Node   │  │
│  │  (porta N)   │  │  (porta N+1) │  │  (cliente)   │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

**Protocolo**: TCP com mensagens JSON delimitadas por newline (`\n`).

**Resiliência**: Múltiplos nós podem rodar o serviço de descoberta simultaneamente.
Se um cair, os outros continuam servindo. Peers também se comunicam via gossip
para propagar informações de outros peers sem depender de um servidor central.

## Estrutura do Projeto

```
p2p/
├── cmd/peer/main.go              # Entrada principal (CLI)
├── internal/
│   ├── protocol/protocol.go     # Tipos de mensagens e encoding
│   ├── discovery/discovery.go   # Serviço de descoberta distribuído
│   ├── fileserver/fileserver.go # Servidor de arquivos (serve downloads)
│   └── peer/peer.go             # Lógica do nó P2P (cliente)
├── Makefile
└── go.mod
```

## Como Compilar

```bash
# Requer Go 1.21+
make build
# Binário gerado em: build/p2p-peer
```

Ou diretamente:
```bash
go build -o p2p-peer ./cmd/peer/
```

## Como Usar

### Portas utilizadas por nó

| Porta   | Serviço                            |
|---------|------------------------------------|
| `-port` | Discovery (registro de peers)      |
| `-port+1` | FileServer (download de arquivos) |

### Exemplo: Dois nós na mesma máquina

**Terminal 1 — Nó bootstrap:**
```bash
./p2p-peer -id node1 -port 6000 -share ./shared/node1 -download ./downloads/node1
```

**Terminal 2 — Segundo nó:**
```bash
./p2p-peer -id node2 -port 6002 -discovery "127.0.0.1:6000" \
           -share ./shared/node2 -download ./downloads/node2
```

### Exemplo: Duas máquinas na rede

**Máquina A (IP: 192.168.1.10):**
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

### Múltiplos discovery nodes (alta disponibilidade)

```bash
# Máquina A e B rodando discovery
./p2p-peer -id maquinaC -port 6000 \
           -discovery "192.168.1.10:6000,192.168.1.20:6000" \
           -share ./compartilhados
```

## Comandos do CLI

| Comando               | Descrição                                      |
|-----------------------|------------------------------------------------|
| `peers`               | Listar peers conhecidos                        |
| `refresh`             | Atualizar lista de peers via discovery         |
| `files <addr>`        | Listar arquivos em um peer específico          |
| `all-files`           | Listar arquivos de todos os peers              |
| `get <addr> <arq>`    | Baixar arquivo diretamente de um peer          |
| `myfiles`             | Ver meus arquivos compartilhados               |
| `quit`                | Sair                                           |

**Exemplo de sessão:**
```
[node2]> refresh
  Lista atualizada.

[node2]> peers
  ID                    Endereço                   Último contato
  -----------------------------------------------------------------
  node1                 192.168.1.10:6001          há 2s

[node2]> files 192.168.1.10:6001
  Peer node1 @ 192.168.1.10:6001:
    relatorio.pdf                          2.3 MB
    apresentacao.pptx                      1.1 MB

[node2]> get 192.168.1.10:6001 relatorio.pdf
  Baixando relatorio.pdf de 192.168.1.10:6001 ...
  Concluído em 234ms  →  ./baixados/relatorio.pdf
```

## Flags Disponíveis

| Flag              | Padrão         | Descrição                                          |
|-------------------|----------------|----------------------------------------------------|
| `-id`             | `hostname-port`| Identificador único deste peer                     |
| `-port`           | `6000`         | Porta base (discovery=N, fileserver=N+1)           |
| `-public-addr`    | auto-detectado | IP/hostname para anunciar a outros peers           |
| `-share`          | `./shared`     | Diretório de arquivos compartilhados               |
| `-download`       | `./downloads`  | Diretório de downloads                             |
| `-discovery`      | (vazio)        | Discovery nodes externos (`host:port,...`)         |
| `-discovery-node` | `false`        | Forçar modo discovery mesmo com peers configurados |

## Testes

```bash
make test
# ou
go test -v ./...
```

Testes cobrem:
- Descoberta de peers
- Transferência de arquivos com verificação MD5
- Tratamento de erros (peer inacessível, arquivo inexistente, path traversal)
- Resiliência com múltiplos discovery nodes

## Protocolo de Comunicação

Todas as mensagens são JSON delimitado por newline sobre TCP:

```json
{"type": "REGISTER", "payload": {"id": "node1", "addr": "192.168.1.10:6001"}}
{"type": "PEER_LIST", "payload": {"peers": [...]}}
{"type": "LIST_FILES", "payload": {}}
{"type": "FILE_LIST",  "payload": {"peer_id": "node1", "files": [{"name":"a.txt","size":123}]}}
{"type": "DOWNLOAD",   "payload": {"filename": "a.txt"}}
{"type": "FILE_DATA",  "payload": {"filename": "a.txt", "size": 123}}
[bytes brutos do arquivo]
[MD5 checksum em hex]
```

## Requisitos Atendidos

| Requisito                              | Status  | Implementação                                   |
|----------------------------------------|---------|--------------------------------------------------|
| 1. Registro com ID, endereço, porta    | ✅      | `MsgRegister` no discovery                      |
| 2. Discovery não é servidor central    | ✅      | Qualquer peer pode ser discovery; múltiplos ok  |
| 3. Resiliência a queda de discovery    | ✅      | Múltiplos nodes + gossip entre peers            |
| 4. Listar peers ativos                 | ✅      | `peers` / `refresh` + TTL de 60s               |
| 5. Lista de arquivos por peer          | ✅      | `MsgListFiles` direto entre peers               |
| 6. Download direto entre peers         | ✅      | TCP direto, sem passar pelo discovery           |
| 7. Comunicação por rede (protocolo)    | ✅      | TCP + JSON (protocolo próprio)                  |
| 8. Funciona em múltiplas máquinas      | ✅      | Flag `-public-addr` + auto-detecção de IP       |
| 9. Pasta específica por máquina        | ✅      | Flag `-share`                                   |
| Tratamento de peer indisponível        | ✅      | Timeout + mensagem de erro clara                |
| Tratamento de arquivo inexistente      | ✅      | Erro do fileserver propagado                    |
| Tratamento de arquivo corrompido       | ✅      | MD5 verificado; arquivo removido se errado      |
| Conexão malsucedida                    | ✅      | `dial` com timeout, erro descritivo             |
| Proteção contra path traversal         | ✅      | `filepath.Base()` no fileserver                |
