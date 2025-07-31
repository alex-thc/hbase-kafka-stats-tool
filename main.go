package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/hamba/avro/v2"
)

var AvroSchema = avro.MustParse(`{"namespace": "org.apache.hadoop.hbase.kafka",
 "type": "record",
 "name": "HBaseKafkaEvent",
 "fields": [
    {"name": "key", "type": "bytes"},
    {"name": "timestamp",  "type": "long" },
    {"name": "delete",  "type": "boolean" },
    {"name": "value", "type": "bytes"},
    {"name": "qualifier", "type": "bytes"},
    {"name": "family", "type": "bytes"},
    {"name": "table", "type": "bytes"}
 ]
}`)

type HbaseKafkaEvent struct {
	Key       []byte `avro:"key"`
	Timestamp int64  `avro:"timestamp"`
	Delete    bool   `avro:"delete"`
	Value     []byte `avro:"value"`
	Qualifier []byte `avro:"qualifier"`
	Family    []byte `avro:"family"`
	Table     []byte `avro:"table"`
}

type Stats struct {
	Count      int
	TotalBytes int
}

func newKafkaConfig(user, password string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V2_1_0_0
	if user != "" {
		cfg.Net.TLS.Enable = true
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypePlaintext)
		cfg.Net.SASL.User = user
		cfg.Net.SASL.Password = password
	}
	return cfg
}

func main() {
	broker := flag.String("broker", "", "Kafka broker address")
	topic := flag.String("topic", "", "Kafka topic")
	user := flag.String("user", "", "Kafka username (optional)")
	password := flag.String("password", "", "Kafka password (optional)")
	duration := flag.Int("duration", 10, "Seconds to consume messages")
	flag.Parse()

	if *broker == "" || *topic == "" {
		fmt.Printf("Usage: %s -broker <broker> -topic <topic> [-user <user>] [-password <password>] [-duration <seconds>]\n", os.Args[0])
		os.Exit(1)
	}

	config := newKafkaConfig(*user, *password)
	client, err := sarama.NewClient([]string{*broker}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(*topic)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	stats := make(map[string]*Stats)
	var statsMu sync.Mutex
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	done := make(chan struct{})
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(*topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Failed to consume partition %d: %v", partition, err)
		}
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				var event HbaseKafkaEvent
				if err := avro.Unmarshal(AvroSchema, message.Value, &event); err != nil {
					log.Printf("Failed to decode message: %v", err)
					continue
				}
				ns := string(event.Table)
				size := len(message.Value)
				statsMu.Lock()
				if stats[ns] == nil {
					stats[ns] = &Stats{}
				}
				stats[ns].Count++
				stats[ns].TotalBytes += size
				statsMu.Unlock()
			}
			done <- struct{}{}
		}(pc)
	}

	fmt.Printf("Consuming messages for %d seconds...\n", *duration)
	time.Sleep(time.Duration(*duration) * time.Second)

	fmt.Println("Namespace statistics:")
	statsMu.Lock()
	for ns, s := range stats {
		avg := 0
		if s.Count > 0 {
			avg = s.TotalBytes / s.Count
		}
		fmt.Printf("Namespace: %s, Events: %d, Avg Size: %d bytes, Total Size: %d bytes\n", ns, s.Count, avg, s.TotalBytes)
	}
	statsMu.Unlock()
}
