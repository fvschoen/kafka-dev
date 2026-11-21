import argparse
import json
import signal
from kafka import KafkaConsumer


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume hello-world events from Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="hello-world")
    parser.add_argument("--group-id", default="hello-world-consumer-group")
    parser.add_argument("--max-messages", type=int, default=0, help="0 means run until interrupted")
    args = parser.parse_args()

    running = True

    def _handle_sigint(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: _decode_value(v),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=1000,
    )

    consumed = 0
    try:
        while running:
            for msg in consumer:
                print(
                    f"Consumed partition={msg.partition} offset={msg.offset} "
                    f"key={msg.key} value={msg.value}"
                )
                consumed += 1
                if args.max_messages > 0 and consumed >= args.max_messages:
                    running = False
                    break
    finally:
        consumer.close()


def _decode_value(raw: bytes | None):
    if raw is None:
        return None
    text = raw.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_text": text}


if __name__ == "__main__":
    main()

