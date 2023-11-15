package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/3a/

func main() {
	n := maelstrom.NewNode()
	messages := []interface{}{}
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		req := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		messages = append(messages, req["message"])
		body := make(map[string]interface{})
		body["type"] = "broadcast_ok"
		return n.Reply(msg, body)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		body["type"] = "read_ok"
		body["messages"] = messages
		return n.Reply(msg, body)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
