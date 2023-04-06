package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

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

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
	})

	msgChan := make(chan *Message, 1000)
	stopChan := make(chan struct{}, cfg.Workers)

	for i := 1; i <= cfg.Workers; i++ {
		go worker(i, msgChan, stopChan)
	}

	go messageReader(r, msgChan)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	for sig := range c {
		switch sig {
		case os.Interrupt, os.Kill:
			stopWorkers(cfg.Workers, stopChan)
			_ = r.Close()
			return
		}
	}
}

type Message struct {
	kafka.Message
	Host string
}

func messageReader(r *kafka.Reader, msgChan chan *Message) {
	log.Printf("starting message reader %v", r.Stats())
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("error reading message: %v", err)
			break
		}
		h, _ := os.Hostname()
		msgChan <- &Message{
			Message: m,
			Host:    h,
		}
	}
}

func worker(idx int, in chan *Message, stop chan struct{}) {
	log.Printf("Starting worker %d", idx)
	for {
		select {
		case msg := <-in:
			log.Printf("worker %d working on message: %v", idx, string(msg.Message.Value))
			log.Printf("sending further that host %v has done something with message...", msg.Host)
		case <-stop:
			log.Printf("stopping worker %d", idx)
			return
		}
	}
}

func stopWorkers(workerNum int, stopChan chan struct{}) {
	for i := 1; i <= workerNum; i++ {
		stopChan <- struct{}{}
	}
}
