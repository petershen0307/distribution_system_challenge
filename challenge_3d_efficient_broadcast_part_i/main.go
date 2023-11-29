package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/3c/

func getMsgID(body json.RawMessage) int {
	msgBody := maelstrom.MessageBody{}
	if err := json.Unmarshal(body, &msgBody); err != nil {
		return 0
	}
	return msgBody.MsgID
}

type maelstromSvc struct {
	node           *maelstrom.Node
	topologyMap    map[string][]string
	messagesLock   sync.RWMutex
	receivedMsgMap map[interface{}]struct{}

	// graceful shutdown
	cancelCtx context.Context
	cancel    context.CancelFunc
	// sender message queue
	msgQueueRWMutex sync.RWMutex
	msgQueue        map[string]map[int]interface{} // string is node id, int is message id
	// snowflake machine information
	machineSerialID int
	machineID       int
}

func newSvc() *maelstromSvc {
	ctx, cancel := context.WithCancel(context.Background())
	svc := &maelstromSvc{
		node:           maelstrom.NewNode(),
		topologyMap:    make(map[string][]string),
		receivedMsgMap: make(map[interface{}]struct{}),
		cancelCtx:      ctx,
		cancel:         cancel,
		msgQueue:       make(map[string]map[int]interface{}),
		machineID:      os.Getpid(),
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
			svc.machineSerialID++
			svc.msgQueueRWMutex.Lock()
			// we add message to queue and the sender will help to send the message to target
			if _, ok := svc.msgQueue[node]; !ok {
				svc.msgQueue[node] = make(map[int]interface{})
			}
			svc.msgQueue[node][getMsgID(msg.Body)] = req
			svc.msgQueueRWMutex.Unlock()
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

func (svc *maelstromSvc) sender() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			svc.msgQueueRWMutex.RLock()
			// send all message in the queue
			for node, messages := range svc.msgQueue {
				for _, broadcastMsg := range messages {
					if err := svc.node.RPC(node, broadcastMsg, func(msg maelstrom.Message) error {
						svc.msgQueueRWMutex.Lock()
						delete(svc.msgQueue[msg.Src], getMsgID(msg.Body))
						svc.msgQueueRWMutex.Unlock()
						return nil
					}); err != nil {
						continue
					}
				}
			}
			svc.msgQueueRWMutex.RUnlock()
		case <-svc.cancelCtx.Done():
			// wait shutdown event
			return
		}
	}
}

func (svc *maelstromSvc) shutdown() {
	svc.cancel()
}

func main() {
	svc := newSvc()
	go svc.sender()
	if err := svc.node.Run(); err != nil {
		fmt.Println(err)
	}
	svc.shutdown()
}
