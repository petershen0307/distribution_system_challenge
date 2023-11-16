package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/3b/

// in this challenge we need to add lock when we write new message to `messages` object or read message from `messages` object
// to guarantee `messages` object can be read and written correctly

func main() {
	messages := []interface{}{}
	messagesMutex := sync.Mutex{}
	topology := make(map[string][]string)

	// n.Handle("init", func(msg maelstrom.Message) error {
	// 	nodes := n.NodeIDs()
	// 	topology[nodes[0]] = nodes[1:]
	// 	for _, node := range nodes[1:] {
	// 		topology[node] = []string{nodes[0]}
	// 	}
	// 	fmt.Println("=============================", topology)
	// 	return nil
	// })

	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		req := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		messagesMutex.Lock()
		messages = append(messages, req["message"])
		messagesMutex.Unlock()

		body := make(map[string]interface{})
		body["type"] = "broadcast_ok"
		for _, node := range topology[n.ID()] {
			err := n.Send(node, req)
			if err != nil {
				return err
			}
		}
		return n.Reply(msg, body)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		body["type"] = "read_ok"
		messagesMutex.Lock()
		body["messages"] = messages
		messagesMutex.Unlock()
		return n.Reply(msg, body)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		/*
			{
				"n0": ["n3","n1"],
				"n1": ["n4","n2","n0"],
				"n2": ["n1"],
				"n3": ["n0","n4"],
				"n4": ["n1","n3"]
			}
		*/
		msgBody := struct {
			Topology map[string][]string `json:"topology"`
		}{}
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}
		for k, v := range msgBody.Topology {
			topology[k] = v
		}

		body := make(map[string]interface{})
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
