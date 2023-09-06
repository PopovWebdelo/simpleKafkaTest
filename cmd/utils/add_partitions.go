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
	newPartitions := currentPartitions + 3 // For example, add 3 more partitions

	err = admin.CreatePartitions(topic, int32(newPartitions), nil, false)
	if err != nil {
		log.Fatalf("Failed to add partitions to the topic: %v", err)
	}

	fmt.Println("Partitions added successfully!")
}
