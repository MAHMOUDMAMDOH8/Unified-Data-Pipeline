from kafka import KafkaProducer
import json
import time
import os
import logging
import random
import sys


CURRENT_DIR = os.path.dirname(__file__)
DAGS_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
if DAGS_ROOT not in sys.path:
    sys.path.append(DAGS_ROOT)

from scripts.Stream.Logs.logs import LogGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('producer')

topic = os.getenv('KAFKA_TOPIC', 'Stock')

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)




def Stream_events(num_event=100):
    """
    Stream events to Kafka topic
    """
    counter = 0
    while counter < num_event:
        try:
            events = LogGenerator().generate_logs(1, interval=0)
            event = events[0]
            event_type = event['event_type']
            producer.send(topic, value=event)
            logger.info(f"Produced event: {event_type}")
            counter += 1
            time.sleep(random.uniform(0.1, 0.5))
        except Exception as e:
            logger.error(f"Error producing event: {e}")
            time.sleep(1)


if __name__ == "__main__":
    # Basic port check (optional). Uses configured host:port if available
    host, port = (bootstrap_servers.split(',')[0].split(':') + ["9092"])[:2]
    try:
        rc = os.system(f"nc -z {host} {port} >/dev/null 2>&1")
        if rc != 0:
            logger.warning("Kafka reachability check failed; attempting to produce anyway")
    except Exception:
        logger.warning("nc not available; skipping Kafka reachability check")

    Stream_events(int(os.getenv('NUM_EVENTS', '100')))
    producer.flush()
    producer.close()

