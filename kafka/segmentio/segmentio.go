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
}

func (r KafkaConfig) NewReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: r.Brokers,
		Topic:   r.Topic,
		GroupID: r.Group,
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

func (w KafkaConfig) Send(key string, message interface{}) error {
	fmt.Println(fmt.Sprintf("kafka url: %v", w.Brokers))
	fmt.Println("kafka topic: " + w.Topic)
	writer := KafkaConfig.NewWriter(KafkaConfig{Brokers: w.Brokers, Topic: w.Topic})
	defer writer.Close()

	data, _ := json.Marshal(message)
	s := string(data)
	s = strings.ReplaceAll(s, `\`, ``)

	// SEGMENTIO
	msg := kafka.Message{
		Topic: w.Topic,
		Key:   []byte(key),
		Value: data,
	}
	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println("fail produce kafka")
		return err
	}
	fmt.Println("produce kafka success")
	return nil
}
