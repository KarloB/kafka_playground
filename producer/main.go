package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/KarloB/kafka_playground/pkg/config"
	"github.com/segmentio/kafka-go"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "configPath", "config.yaml", "Path to configuration")
	flag.Parse()

	cfg, err := config.GetConfig(configPath)
	if err != nil {
		panic(err)
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", cfg.Brokers[0], cfg.Topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	for {
		numOfMessages := rand.Intn(50-5) + 5
		messages := make([]kafka.Message, numOfMessages)
		for i := 0; i < numOfMessages; i++ {
			num := rand.Intn(100000-1000) + 1000
			tmp := struct {
				Somevalue int `json:"somevalue"`
			}{
				Somevalue: num,
			}
			b := new(bytes.Buffer)
			if err := json.NewEncoder(b).Encode(tmp); err != nil {
				panic(err)
			}
			messages[i].Key = []byte(fmt.Sprintf("MessageIndex_%d", i))
			messages[i].Value = b.Bytes()
		}

		log.Printf("Sending %d messages", numOfMessages)
		if _, err := conn.WriteMessages(messages...); err != nil {
			panic(err)
		}

		sleepTime := time.Millisecond * time.Duration(rand.Intn(1000-100)+100)
		time.Sleep(sleepTime)

	}

}
