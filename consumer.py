from kafka import KafkaConsumer, KafkaProducer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP = "172.20.220.97:9092"

consumer = KafkaConsumer(
    "eventos",
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="grupo-pagos"
)

dlq_producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Esperando mensajes...")

for mensaje in consumer:
    try:
        evento = mensaje.value

        if evento["tipo"] == "pago":
            print(f"💳 Pago procesado — ${evento['monto']} de {evento['usuario']}")
        elif evento["tipo"] == "retiro":
            print(f"🏧 Retiro procesado — ${evento['monto']} de {evento['usuario']}")
        else:
            raise ValueError(f"Tipo desconocido: {evento['tipo']}")

    except Exception as e:
        logger.error(f"Enviando a DLQ — offset {mensaje.offset}: {e}")
        dlq_producer.send("eventos-fallidos", value={
            "error": str(e),
            "offset_original": mensaje.offset,
            "evento": mensaje.value
        })
        dlq_producer.flush()