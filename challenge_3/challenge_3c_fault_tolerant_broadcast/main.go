package main

import (
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/3c/

type maelstromSvc struct {
	node            *maelstrom.Node
	topologyMap     map[string][]string
	messagesRWMutex sync.RWMutex
	messages        []interface{}
}

func newSvc() *maelstromSvc {
	svc := &maelstromSvc{
		node:        maelstrom.NewNode(),
		topologyMap: make(map[string][]string),
		messages:    make([]interface{}, 0),
	}
	svc.node.Handle("broadcast", svc.broadcast)
	svc.node.Handle("read", svc.read)
	svc.node.Handle("topology", svc.topology)
	return svc
}

func (svc *maelstromSvc) broadcast(msg maelstrom.Message) error {
	req := make(map[string]interface{})
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	svc.messagesRWMutex.Lock()
	svc.messages = append(svc.messages, req["message"])
	svc.messagesRWMutex.Unlock()

	for _, node := range svc.topologyMap[svc.node.ID()] {
		svc.node.Send(node, req["message"])
	}

	body := make(map[string]interface{})
	body["type"] = "broadcast_ok"

	return svc.node.Reply(msg, body)
}

func (svc *maelstromSvc) read(msg maelstrom.Message) error {
	body := make(map[string]interface{})
	body["type"] = "read_ok"
	svc.messagesRWMutex.RLock()
	body["messages"] = svc.messages
	svc.messagesRWMutex.RUnlock()
	return svc.node.Reply(msg, body)
}

func (svc *maelstromSvc) topology(msg maelstrom.Message) error {
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
		svc.topologyMap[k] = v
	}

	body := make(map[string]interface{})
	body["type"] = "topology_ok"
	return svc.node.Reply(msg, body)
}

func main() {
	svc := newSvc()
	if err := svc.node.Run(); err != nil {
		fmt.Println(err)
	}
}
