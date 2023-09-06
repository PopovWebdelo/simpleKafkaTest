package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

func main() {
	brokerList := []string{"localhost:29092"}
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka admin: %v", err)
	}
	defer admin.Close()

	topic := "my_topic"
	description, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		log.Fatalf("Failed to describe topic: %v", err)
	}

	currentPartitions := len(description[0].Partitions)

	fmt.Printf("Topic \"%s\" has %d partitions.", topic, currentPartitions)
}
