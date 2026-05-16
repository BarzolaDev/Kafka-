import json
import pytest
from testcontainers.kafka import KafkaContainer
from kafka import KafkaProducer, KafkaConsumer


@pytest.fixture(scope="module")
def kafka():
    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


def test_producer_publica_mensaje(kafka):
    producer = KafkaProducer(
        bootstrap_servers=kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    future = producer.send("test-eventos", value={"tipo": "pago", "monto": 100})
    result = future.get(timeout=10)

    assert result.topic == "test-eventos"
    assert result.offset == 0
    producer.close()


def test_consumer_recibe_mensaje(kafka):
    producer = KafkaProducer(
        bootstrap_servers=kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send("test-pagos", value={"tipo": "pago", "monto": 500})
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        "test-pagos",
        bootstrap_servers=kafka,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="test-group",
        consumer_timeout_ms=5000
    )

    mensajes = [m.value for m in consumer]
    consumer.close()

    assert len(mensajes) == 1
    assert mensajes[0]["tipo"] == "pago"
    assert mensajes[0]["monto"] == 500