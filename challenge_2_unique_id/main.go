package main

// challenge https://fly.io/dist-sys/2/

import (
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/petershen0307/distribution_system_challenge/snowflakeid"
)

func main() {
	n := maelstrom.NewNode()
	serialID := 0
	n.Handle("generate", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		body["type"] = "generate_ok"
		body["id"] = snowflakeid.GenerateSnowflakeID(time.Now().UTC(), os.Getpid(), serialID)
		serialID++
		if serialID == snowflakeid.MaximumSerialID {
			serialID = 0
		}
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
