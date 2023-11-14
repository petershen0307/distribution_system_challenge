package main

// challenge https://fly.io/dist-sys/2/

import (
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maximumMachineID = 2 << 10
const maximumSerialID = 2 << 12

// 41 bits for timestamp, 10 bits for machine ID, 12 bits for serial ID
const timestampMask = 0x000003FFFFFFFFFF
const machineIDMask = 0x000FFC0000000000
const serialIDMask = 0xFFF0000000000000

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		body["type"] = "generate_ok"
		body["id"] = generateSnowflakeID()
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func generateSnowflakeID() uint64 {
	// https://en.wikipedia.org/wiki/Snowflake_ID
	timestampMS := time.Now().UTC().UnixMilli()

	return (uint64(timestampMS) & timestampMask) | ((uint64(rand.Intn(maximumMachineID)) << 41) & machineIDMask) | ((uint64(rand.Intn(maximumSerialID)) << 51) & serialIDMask)
}
