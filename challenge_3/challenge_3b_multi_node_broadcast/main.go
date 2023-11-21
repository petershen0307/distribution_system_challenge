package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/3b/

type maelstromSvc struct {
	node           *maelstrom.Node
	topologyMap    map[string][]string
	messagesLock   sync.RWMutex
	receivedMsgMap map[interface{}]struct{}
}

func newSvc() *maelstromSvc {
	svc := &maelstromSvc{
		node:           maelstrom.NewNode(),
		topologyMap:    make(map[string][]string),
		receivedMsgMap: make(map[interface{}]struct{}),
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
	newMsg := req["message"]
	svc.messagesLock.Lock()
	_, ok := svc.receivedMsgMap[newMsg]
	svc.receivedMsgMap[newMsg] = struct{}{}
	svc.messagesLock.Unlock()
	if !ok {
		for _, node := range svc.topologyMap[svc.node.ID()] {
			if msg.Src == node {
				// skip broadcasting to the node that sent the message
				continue
			}
			if err := svc.node.Send(node, req); err != nil {
				return err
			}
		}
	}

	body := make(map[string]interface{})
	body["type"] = "broadcast_ok"

	return svc.node.Reply(msg, body)
}

func (svc *maelstromSvc) read(msg maelstrom.Message) error {
	body := make(map[string]interface{})
	body["type"] = "read_ok"
	svc.messagesLock.RLock()
	messages := []interface{}{}

	for k := range svc.receivedMsgMap {
		messages = append(messages, k)
	}
	body["messages"] = messages
	svc.messagesLock.RUnlock()
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
	log.SetOutput(os.Stderr)
	svc := newSvc()
	if err := svc.node.Run(); err != nil {
		fmt.Println(err)
	}
}
