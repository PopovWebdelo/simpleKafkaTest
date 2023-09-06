package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	ready chan bool
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	wait := &sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		wait.Add(1)
		go func(mChan <-chan *sarama.ConsumerMessage, num int, wait *sync.WaitGroup) {
			count := 0
			writer := bufio.NewWriter(os.Stdout)
			defer writer.Flush()

			for message := range mChan {
				now := time.Now()
				stringTime := string(message.Value)
				sendingTime, _ := time.Parse(time.RFC3339, stringTime)
				sendingDuration := now.Sub(sendingTime)

				headers := ""
				for _, h := range message.Headers {
					headers += "{ Key: " + string(h.Key) + ", Value: " + string(h.Value) + "},"
				}

				strToOut := time.Now().Format("2006-01-02 15:04:05") + ": Offset = " +
					strconv.FormatInt(message.Offset, 10) + " | Partition = " +
					strconv.FormatInt(int64(message.Partition), 10) + " | Milliseconds = " +
					strconv.FormatInt(sendingDuration.Milliseconds(), 10) + " | Microseconds = " +
					strconv.FormatInt(sendingDuration.Microseconds(), 10) + " | Headers: " +
					headers + "\n"
				writer.WriteString(strToOut)

				session.MarkMessage(message, "")
				count++
				if count == 1_000_000 {
					writer.Flush()
					count = 0
				}
			}

			wait.Done()
		}(claim.Messages(), i, wait)
	}
	wait.Wait()
	return nil
}

func main() {
	kafkaAddress := "0.0.0.0:29092" // Адрес брокера Kafka внутри Docker
	topic := "my_topic"             // Топик, куда вы отправляли сообщения

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // Замените на поддерживаемую версию Kafka
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Group.Session.Timeout = 30 * time.Second

	// Настройка параметров
	config.Consumer.Fetch.Default = 1024 * 1024          // например, 1MB
	config.Consumer.Fetch.Max = 10 * 1024 * 1024         // максимальное количество данных, которое будет извлечено за один раз
	config.Consumer.MaxWaitTime = time.Millisecond * 100 // fetch.max.wait.ms

	client, err := sarama.NewConsumerGroup([]string{kafkaAddress}, "my-group", config)
	if err != nil {
		log.Fatalln("Error creating consumer group client:", err)
	}
	defer client.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
		case sig := <-signals:
			log.Printf("Received signal %s, stopping...", sig)
			cancel()
		}
	}()

	consumer := &Consumer{
		ready: make(chan bool),
	}

	go func() {
		for {
			err := client.Consume(ctx, []string{topic}, consumer)
			if err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready

	fmt.Println("Consumer is ready. Press Ctrl+C to exit.")
	<-ctx.Done()
	fmt.Println("Exiting...")
}
