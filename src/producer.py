import argparse
from datetime import datetime, timezone
import json
import time
from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish hello-world events to Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="hello-world")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    try:
        for i in range(args.count):
            key = f"worker-{i % 2}"
            payload = {
                "seq": i,
                "message": f"Hello Kafka #{i}",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            future = producer.send(args.topic, key=key, value=payload)
            meta = future.get(timeout=10)
            print(
                f"Produced seq={i} key={key} -> topic={meta.topic} "
                f"partition={meta.partition} offset={meta.offset}"
            )
            time.sleep(args.interval)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()


