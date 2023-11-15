.PHONY: challenge_1_echo

WORKSPACE := workspace
BIN := bin
MAELSTROM_BIN := ./maelstrom/maelstrom
WORKSPACE_OUTBIN_DIR := ./$(WORKSPACE)/$(BIN)

setup:
	mkdir -p $(WORKSPACE_OUTBIN_DIR)

setup_tool:
	asdf plugin-add java https://github.com/halcyon/asdf-java.git
	asdf install java openjdk-21
	asdf global java openjdk-21
	sudo apt install graphviz gnuplot

setup_maelstrom: setup
	wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
	tar -jxv -f maelstrom.tar.bz2 -C ./$(WORKSPACE)
	rm maelstrom.tar.bz2

build: setup
	go build -o $(WORKSPACE_OUTBIN_DIR) ./...

# folder store is the maelstrom cache result
clean:
	rm -rf $(WORKSPACE_OUTBIN_DIR)
	rm -rf $(WORKSPACE)/store

run_maelstrom:
	cd $(WORKSPACE) && $(MAELSTROM_BIN) serve

challenge_1_echo: build
	cd $(WORKSPACE) && $(MAELSTROM_BIN) test -w echo --bin ./$(BIN)/$@ --node-count 1 --time-limit 10

challenge_2_unique_id: build
	cd $(WORKSPACE) && $(MAELSTROM_BIN) test -w unique-ids --bin ./$(BIN)/$@ --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
