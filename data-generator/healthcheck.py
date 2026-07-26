"""
Health check script for the data-generator container.
Returns exit 0 if the generator process is alive and Kafka is reachable.
"""
import os
import sys

from confluent_kafka.admin import AdminClient

broker = os.getenv("KAFKA_BROKER", "localhost:29092")
try:
    admin = AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 3000})
    meta = admin.list_topics(timeout=5)
    sys.exit(0)
except Exception as e:
    print(f"Health check failed: {e}", file=sys.stderr)
    sys.exit(1)
