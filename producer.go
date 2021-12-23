package main

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ctx := context.Background()
	produce(ctx)
}

func produce(ctx context.Context) {

	configMap := make(map[string]kafka.ConfigValue)
	configMap["bootstrap.servers"] = "localhost:9092" // on host, outside docker container
	var kConfigMap kafka.ConfigMap = kafka.ConfigMap(configMap)

	p, err := kafka.NewProducer(&kConfigMap)
	if err != nil {
		panic(err)
	}

	go func() { // async ack update
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Message failed to deliver %v \n", ev.TopicPartition.Error.Error())
				} else {
					fmt.Printf("Message delivered sucess %v \n", ev.TopicPartition.Offset.String())
				}
			}
		}
	}()

	topic := "topic_name" // topic must exit or use adminClient to create one
	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: 0,
	}

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := []string{"value 1", "value 2", "value 3", "value 4", "value 5"}

	for i := range keys {

		kafkaMsg := kafka.Message{TopicPartition: topicPartition, Value: []byte(values[i]), Key: []byte(keys[i])}
		p.Produce(&kafkaMsg, nil)
	}

	p.Flush(25000) // wait
}
