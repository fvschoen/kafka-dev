# kafka-dev

A minimal, reproducible local Apache Kafka starter repository in Python, designed as a clean baseline for streaming experiments, consumer resilience, and downstream data/ML pipelines.

## Architecture & Flow

```text
src/producer.py ──▶ Kafka Broker (KRaft, localhost:9092) ──▶ src/consumer.py
                      └── topic: hello-world (partitioned log)
```

- **Broker (`docker-compose.yml`):** Runs a single-node Apache Kafka broker in KRaft mode (no ZooKeeper required) on port `9092`.
- **Producer (`src/producer.py`):** Generates keyed JSON events (`seq`, `message`, `timestamp_utc`) and writes them to Kafka with `acks="all"`.
- **Consumer (`src/consumer.py`):** Subscribes to a topic under a consumer group, decodes JSON payloads with a plain-text fallback, tracks offsets, and shuts down cleanly on signals or `--max-messages`.

---

## Repository Layout

```text
kafka-dev/
├── docker-compose.yml
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── producer.py
│   └── consumer.py
├── .gitignore
└── README.md
```

---

## Key Kafka Concepts

- **Topic:** A named, durable append-only event log (here, `hello-world`). Unlike traditional work queues, messages remain in the log and are not deleted upon consumption.
- **Partition:** A physical subdivision of a topic that enables parallelism and ordering guarantees. Messages sharing the same **message key** hash to the same partition so per-key ordering is preserved.
- **Offset:** A monotonically increasing integer assigned to each message within a partition (`0, 1, 2, ...`). It identifies a record's exact position in the log.
- **Consumer Group (`group-id`):** One or more consumers cooperating to read a topic. Kafka tracks committed offsets per consumer group so consumers can resume where they left off or replay history using a new group ID.
- **Producer Acks (`acks="all"`):** Controls durability guarantees on write—waiting for leader and in-sync replica acknowledgment before treating a send as committed.

---

## Code & Function Reference

### `src/producer.py`

- **`main()`:**
  - Parses CLI flags (`--bootstrap-servers`, `--topic`, `--count`, `--interval`) using `argparse`.
  - Initializes a `KafkaProducer` with UTF-8 key encoding, JSON value serialization (`json.dumps`), and `acks="all"`.
  - Loops `count` times, assigning a rotating key (`worker-0` / `worker-1`) and a JSON payload containing `seq`, `message`, and an ISO-8601 UTC `timestamp_utc`.
  - Calls `producer.send(...)` and blocks on `future.get(timeout=10)` to log the assigned `topic`, `partition`, and `offset`, sleeping `interval` seconds between events.
  - Flushes pending writes and closes the producer cleanly in a `finally` block.

### `src/consumer.py`

- **`_decode_value(raw: bytes | None)`:**
  - Safe consumer-side deserializer helper. Returns `None` if `raw` is `None`, decodes UTF-8 bytes, attempts `json.loads(text)`, and falls back to `{"raw_text": text}` on `json.JSONDecodeError` so non-JSON messages (such as raw CLI test lines) do not crash the consumer loop.
- **`_handle_sigint(_signum, _frame)`:**
  - Signal handler callback registered for `SIGINT` and `SIGTERM` inside `main()` that sets `running = False` so the consumer exits its poll loop cleanly.
- **`main()`:**
  - Parses CLI flags (`--bootstrap-servers`, `--topic`, `--group-id`, `--max-messages`) using `argparse`.
  - Registers signal handlers (`SIGINT`, `SIGTERM`) via `_handle_sigint` to flip `running = False` for graceful shutdown.
  - Initializes a `KafkaConsumer` configured with `auto_offset_reset="earliest"`, `enable_auto_commit=True`, key/value deserializers, and `consumer_timeout_ms=1000` so polling loops can check `running` periodically.
  - Iterates over incoming records, prints `partition`, `offset`, `key`, and decoded `value`, stops once `--max-messages` is reached (if greater than `0`), and closes the consumer in a `finally` block.

---

## Quick Start

### 1. Start the Kafka Broker

```bash
docker compose up -d
docker compose ps
```

### 2. Create the Topic (First Time Only)

```bash
docker exec kafka-dev-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic hello-world --partitions 1 --replication-factor 1
```

### 3. Set Up Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Produce and Consume Messages

```bash
python -m src.producer --count 5
python -m src.consumer --max-messages 5
```

### 5. Tear Down

```bash
docker compose down
```

---

## Planned Extensions

- Multi-partition topics with parallel consumers in the same consumer group.
- Dead-letter routing or strict schema validation for malformed payloads.
- Downstream Spark Structured Streaming or telemetry/ML pipeline integration.
