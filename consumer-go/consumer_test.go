package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestConsumerRecibeEvento(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx, "confluentinc/cp-kafka:7.6.0",
    kafka.WithClusterID("test-cluster-id-123456789012"),
	)
	if err != nil {
		t.Fatalf("Error levantando Kafka: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("Error obteniendo brokers: %v", err)
	}

	// Publicar mensaje
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		t.Fatalf("Error creando producer: %v", err)
	}
	defer producer.Close()

	evento := Evento{Tipo: "pago", Monto: 1000, Usuario: "test"}
	data, _ := json.Marshal(evento)

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "eventos",
		Value: sarama.ByteEncoder(data),
	})
	if err != nil {
		t.Fatalf("Error enviando mensaje: %v", err)
	}

	// Consumir mensaje
	consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
	if err != nil {
		t.Fatalf("Error creando consumer: %v", err)
	}
	defer consumer.Close()

	partition, err := consumer.ConsumePartition("eventos", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("Error consumiendo partición: %v", err)
	}
	defer partition.Close()

	select {
	case msg := <-partition.Messages():
		var recibido Evento
		json.Unmarshal(msg.Value, &recibido)

		if recibido.Tipo != "pago" {
			t.Errorf("esperaba 'pago', got '%s'", recibido.Tipo)
		}
		if recibido.Monto != 1000 {
			t.Errorf("esperaba 1000, got %f", recibido.Monto)
		}

	case <-time.After(30 * time.Second):
		t.Fatal("timeout esperando mensaje")
	}
}