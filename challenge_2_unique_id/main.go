package main

// challenge https://fly.io/dist-sys/2/

import (
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const timestampBits = 41
const machineIDBits = 10
const serialIDBits = 12
const maximumSerialID = 2 << serialIDBits

// 41 bits for timestamp, 10 bits for machine ID, 12 bits for serial ID
const timestampMask = uint64(0x0000_03FF_FFFF_FFFE)
const machineIDMask = uint64(0x000F_FC00_0000_0000)
const serialIDMask = uint64(0xFFF0_0000_0000_0000)

func main() {
	n := maelstrom.NewNode()
	serialID := 0
	n.Handle("generate", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		body["type"] = "generate_ok"
		body["id"] = generateSnowflakeID(time.Now().UTC(), os.Getpid(), serialID)
		serialID++
		if serialID == maximumSerialID {
			serialID = 0
		}
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func generateSnowflakeID(timestamp time.Time, machineID, machineSerialID int) uint64 {
	// https://en.wikipedia.org/wiki/Snowflake_ID

	return (uint64(timestamp.UTC().UnixMilli()) << 1 & timestampMask) | ((uint64(machineID) << (1 + timestampBits)) & machineIDMask) | ((uint64(machineSerialID) << (1 + timestampBits + machineIDBits)) & serialIDMask)
}
