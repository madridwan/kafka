package segmentio

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	Brokers []string
	Topic   string
	Group   string
	Key     string
	Message string
}

type SendMessage struct {
	Close chan error
	err   chan error
}

func (w KafkaConfig) NewReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: w.Brokers,
		Topic:   w.Topic,
		GroupID: w.Group,
		// MaxWait:     maxWait,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,    // 1b
		MaxBytes:    10e6, // 10MB
	})
}

func (w KafkaConfig) NewWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  w.Brokers,
		Topic:    w.Topic,
		Balancer: &kafka.CRC32Balancer{},
	})
}

func (w KafkaConfig) Send() error {
	writer := *KafkaConfig.NewWriter(*&KafkaConfig{})
	defer writer.Close()

	data, _ := json.Marshal(w.Message)
	s := string(data)
	s = strings.ReplaceAll(s, `\`, ``)

	// // SEGMENTIO
	// defer writer.Close()
	msg := kafka.Message{
		Topic: w.Topic,
		Key:   []byte(w.Key),
		Value: data,
	}
	err := KafkaConfig.NewWriter(*&KafkaConfig{}).WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println("fail produce kafka")
	} else {
		fmt.Println("produce kafka success")
	}
	return err
}
