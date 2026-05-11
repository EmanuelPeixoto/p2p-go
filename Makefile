BINARY = p2p-peer
BUILD_DIR = build

.PHONY: all build test clean run-bootstrap run-peer2 demo

all: build

build:
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/peer/

test:
	go test -v -timeout 60s ./...

clean:
	rm -rf $(BUILD_DIR)/ shared/ downloads/ *.log

# Rodar nó bootstrap (discovery + fileserver nas portas 6000/6001)
run-bootstrap:
	mkdir -p shared/node1 downloads/node1
	$(BUILD_DIR)/$(BINARY) \
		-id node1 \
		-port 6000 \
		-share shared/node1 \
		-download downloads/node1

# Rodar segundo nó conectando ao bootstrap local
run-peer2:
	mkdir -p shared/node2 downloads/node2
	$(BUILD_DIR)/$(BINARY) \
		-id node2 \
		-port 6002 \
		-discovery "127.0.0.1:6000" \
		-share shared/node2 \
		-download downloads/node2

# Demo rápida com dois nós locais (use dois terminais)
demo:
	@echo "=== Para demo local ==="
	@echo "Terminal 1: make run-bootstrap"
	@echo "Terminal 2: make run-peer2"
	@echo ""
	@echo "=== Para múltiplas máquinas ==="
	@echo "Máquina A (IP 192.168.1.10):"
	@echo "  ./$(BINARY) -id maquinaA -port 6000 -public-addr 192.168.1.10 -share ./shared"
	@echo ""
	@echo "Máquina B:"
	@echo "  ./$(BINARY) -id maquinaB -port 6000 -public-addr 192.168.1.20 \\"
	@echo "              -discovery 192.168.1.10:6000 -share ./shared"
