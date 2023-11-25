package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/petershen0307/distribution_system_challenge/snowflakeid"
)

// https://fly.io/dist-sys/3c/

type parcel struct {
	message    interface{}
	targetNode string
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
	msgQueue        map[uint64]parcel
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
		msgQueue:       make(map[uint64]parcel),
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
			svc.msgQueue[snowflakeid.GenerateSnowflakeID(time.Now(), svc.machineID, svc.machineSerialID)] = parcel{
				targetNode: node,
				message:    req,
			}
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
			for uid, v := range svc.msgQueue {
				msgBodyWithUid := v.message.(map[string]interface{})
				// add my_uid field to memorize the unique id, this id will use in rpc callback to delete sent message in queue
				// because rpc didn't return the msg_id, so I create a myself unique id
				msgBodyWithUid["my_uid"] = uid
				if err := svc.node.RPC(v.targetNode, msgBodyWithUid, func(msg maelstrom.Message) error {
					msgBody := make(map[string]interface{})
					if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
						return err
					}
					// check the my_uid is existed
					if my_uid, exist := msgBody["my_uid"]; exist {
						svc.msgQueueRWMutex.Lock()
						delete(svc.msgQueue, my_uid.(uint64))
						svc.msgQueueRWMutex.Unlock()
					}
					return nil
				}); err != nil {
					continue
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
