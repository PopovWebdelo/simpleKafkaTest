package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	kafkaAddress := "0.0.0.0:29092" // Адрес брокера Kafka внутри Docker

	// Настройка конфигурации продюсера
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Настройка батчинга
	config.Producer.Flush.Frequency = time.Millisecond * 1000 // частота отправки пакетов (например, каждые 500 мс)
	config.Producer.Flush.Messages = 10

	// Создание продюсера
	producer, err := sarama.NewSyncProducer([]string{kafkaAddress}, config)
	if err != nil {
		log.Fatalln("Failed to start producer:", err)
	}
	defer producer.Close()

	// Генерация UUID и временной метки
	//eventUUID := uuid.New()

	// Отправка сообщения в топик
	topic := "my_topic"

	//ticker := time.NewTicker(time.Millisecond * 10)
	//defer ticker.Stop()

	fmt.Println("Start event generation:")
	for {
		maxMessages := 100_000
		msgList := make([]*sarama.ProducerMessage, 0, maxMessages)
		for i := 0; i < maxMessages; i++ {
			msgList = append(msgList, &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(time.Now().Format(time.RFC3339)),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("My_Name"),
						Value: []byte("Andrey_" + strconv.Itoa(rand.Int())),
					},
					{
						Key:   []byte("My_Sur"),
						Value: []byte("Popov_" + strconv.Itoa(rand.Int())),
					},
				},
			})
		}

		err := producer.SendMessages(msgList)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			//fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
			fmt.Printf("Was sent %d messages in banch\n", maxMessages)
		}
	}
}
