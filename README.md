# Kaafka

Aprendiendo Apache Kafka desde cero con Python.

## Stack
- Apache Kafka (KRaft, sin Zookeeper)
- kafka-python
- Testcontainers
- Docker

## Qué hace
- Producer publica eventos de pagos y retiros
- Consumer los procesa en tiempo real
- Mensajes inválidos van a una Dead Letter Queue (`eventos-fallidos`)
- Tests de integración con Kafka real (sin mocks)

## Correr el proyecto

```bash
docker compose up -d
python3 consumer.py  # terminal 1
python3 producer.py  # terminal 2
```

## Correr los tests

```bash
python3 -m pytest test_kafka.py -v
```