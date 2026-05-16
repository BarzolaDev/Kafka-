# Kaafka

Aprendiendo Apache Kafka desde cero con Python y Go.

## Stack
- Apache Kafka (KRaft, sin Zookeeper)
- kafka-python
- Go + Sarama
- Testcontainers
- Docker

## Qué hace
- Producer publica eventos de pagos y retiros
- Consumer en Python procesa eventos en tiempo real con Dead Letter Queue
- Consumer en Go para alta performance
- Mensajes inválidos van a `eventos-fallidos` (DLQ)
- Tests de integración con Kafka real (sin mocks)

## Correr el proyecto

```bash
docker compose up -d
```

Consumer Python:
```bash
python3 consumer.py  # terminal 1
python3 producer.py  # terminal 2
```

Consumer Go:
```bash
cd consumer-go
go run main.go       # terminal 1
cd .. && python3 producer.py  # terminal 2
```

## Correr los tests

```bash
python3 -m pytest test_kafka.py -v
```