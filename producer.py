from kafka import KafkaProducer
import json
import random

producer = KafkaProducer(
    bootstrap_servers="172.20.220.97:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

eventos = [
    {"tipo": "pago", "monto": random.randint(100, 5000), "usuario": "Maxi"},
    {"tipo": "retiro", "monto": random.randint(100, 2000), "usuario": "Maxi"},
    {"tipo": "pago", "monto": random.randint(100, 5000), "usuario": "Maxi"},
    {"tipo": "transferencia", "monto": 999, "usuario": "juan"}
]

for evento in eventos:
    future = producer.send("eventos", value=evento)
    result = future.get(timeout=10)
    print(f"Enviado offset {result.offset} — {evento}")

producer.flush()
producer.close()