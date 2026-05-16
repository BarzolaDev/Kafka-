package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

const BOOTSTRAP = "172.20.220.97:9092"

type Evento struct {
	Tipo    string  `json:"tipo"`
	Monto   float64 `json:"monto"`
	Usuario string  `json:"usuario"`
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer, err := sarama.NewConsumer([]string{BOOTSTRAP}, config)
	if err != nil {
		log.Fatalf("Error creando consumer: %v", err)
	}
	defer consumer.Close()

	partition, err := consumer.ConsumePartition("eventos", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Error consumiendo partición: %v", err)
	}
	defer partition.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Println("Esperando mensajes...")

	for {
		select {
		case msg := <-partition.Messages():
			var evento Evento
			if err := json.Unmarshal(msg.Value, &evento); err != nil {
				log.Printf("Error deserializando: %v", err)
				continue
			}

			switch evento.Tipo {
			case "pago":
				fmt.Printf("💳 Pago procesado — $%.0f de %s\n", evento.Monto, evento.Usuario)
			case "retiro":
				fmt.Printf("🏧 Retiro procesado — $%.0f de %s\n", evento.Monto, evento.Usuario)
			default:
				log.Printf("Tipo desconocido: %s — offset: %d\n", evento.Tipo, msg.Offset)
			}

		case <-signals:
			fmt.Println("Cerrando consumer...")
			return
		}
	}
}