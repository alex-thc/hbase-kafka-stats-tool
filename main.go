package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	Count       int
	TotalBytes  int
	DeleteCount int
	DeleteBytes int
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

	avroConfig := avro.Config{MaxByteSliceSize: -1}.Freeze() //TODO: dsync should be updated
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(*topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Failed to consume partition %d: %v", partition, err)
		}
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				var event HbaseKafkaEvent
				if err := avroConfig.Unmarshal(AvroSchema, message.Value, &event); err != nil {
					log.Printf("Failed to decode message: %v", err)
					continue
				}
				ns := string(event.Table)
				size := len(message.Value)
				statsMu.Lock()
				if stats[ns] == nil {
					stats[ns] = &Stats{}
				}
				if event.Delete {
					stats[ns].DeleteCount++
					stats[ns].DeleteBytes += size
				} else {
					stats[ns].Count++
					stats[ns].TotalBytes += size
				}
				statsMu.Unlock()
			}
			done <- struct{}{}
		}(pc)
	}

	fmt.Printf("Consuming messages. Press Ctrl+C to exit. Printing stats every %d seconds...\n", *duration)

	ticker := time.NewTicker(time.Duration(*duration) * time.Second)
	defer ticker.Stop()

	sig := make(chan os.Signal, 1)
	// Listen for interrupt (Ctrl+C)
	signal.Notify(sig, os.Interrupt)

	for {
		select {
		case <-ticker.C:
			fmt.Printf("\n[%s] Namespace statistics:\n", time.Now().Format(time.RFC3339))
			statsMu.Lock()
			totalEvents := 0
			for _, s := range stats {
				totalEvents += s.Count + s.DeleteCount
			}
			fmt.Printf("Total events: %d\n", totalEvents)
			for ns, s := range stats {
				avg := 0
				if s.Count > 0 {
					avg = s.TotalBytes / s.Count
				}
				deleteAvg := 0
				if s.DeleteCount > 0 {
					deleteAvg = s.DeleteBytes / s.DeleteCount
				}
				fmt.Printf("Namespace: %s, Events: %d, Avg Size: %d bytes, Total Size: %d bytes, Deletes: %d, Delete Avg Size: %d bytes, Delete Total Size: %d bytes\n",
					ns, s.Count, avg, s.TotalBytes, s.DeleteCount, deleteAvg, s.DeleteBytes)
			}
			statsMu.Unlock()
		case <-sig:
			fmt.Println("\nInterrupted. Exiting.")
			// Write stats to CSV on exit
			f, err := os.Create("stats.csv")
			if err != nil {
				log.Printf("Failed to create stats.csv: %v", err)
				return
			}
			defer f.Close()
			w := csv.NewWriter(f)
			defer w.Flush()
			w.Write([]string{"Namespace", "Events", "AvgSize(bytes)", "TotalSize(bytes)", "Deletes", "DeleteAvgSize(bytes)", "DeleteTotalSize(bytes)"})
			statsMu.Lock()
			for ns, s := range stats {
				avg := 0
				if s.Count > 0 {
					avg = s.TotalBytes / s.Count
				}
				deleteAvg := 0
				if s.DeleteCount > 0 {
					deleteAvg = s.DeleteBytes / s.DeleteCount
				}
				w.Write([]string{
					ns,
					fmt.Sprintf("%d", s.Count),
					fmt.Sprintf("%d", avg),
					fmt.Sprintf("%d", s.TotalBytes),
					fmt.Sprintf("%d", s.DeleteCount),
					fmt.Sprintf("%d", deleteAvg),
					fmt.Sprintf("%d", s.DeleteBytes),
				})
			}
			statsMu.Unlock()
			return
		}
	}
}
