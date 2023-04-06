package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
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

		if _, err := conn.WriteMessages(kafka.Message{Value: b.Bytes()}); err != nil {
			panic(err)
		}
		sleepTime := time.Millisecond * time.Duration(rand.Intn(1000-100)+100)
		time.Sleep(sleepTime)

	}

}
