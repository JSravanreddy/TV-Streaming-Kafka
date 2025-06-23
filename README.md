# TV‑Streaming Synthetic Data Stack

A lightweight **end‑to‑end demo** that generates realistic Over‑The‑Top (OTT) / TV‑streaming telemetry, pumps it through **Kafka**, and shows how to ingest the stream with **Apache Spark** for downstream analytics or dashboards.

> Perfect for PoCs, meet‑ups, or interview walkthroughs where you need a complete, running pipeline in minutes.

---

## 📂 Repository Layout

```
.
├── data_generator.py   # Creates fake but coherent streaming events (Python class)
├── kafka_producer.py   # Sends those events to a Kafka topic
├── spark_consumer.py   # Structured Streaming job that reads from Kafka and writes Parquet
├── main.py             # One‑click runner that wires generator → producer → consumer
└── README.md           # You are here 📝
```

Add a `requirements.txt` file if you plan to pin versions; otherwise the install commands below will grab the latest compatible libs.

---

## 🗃️ Event Schema (superset)

| Field              | Type   | Example                    | Notes                                 |
| ------------------ | ------ | -------------------------- | ------------------------------------- |
| `event`            | enum   | `channel_start`            | 11 built‑in event types               |
| `timestamp`        | string | `2025‑06‑23T17:35:12.123Z` | ISO‑8601                              |
| `userId`           | string | `U_1234567`                | Random 7‑digit suffix                 |
| `householdId`      | string | `HH_9876`                  | ―                                     |
| `tvId`             | string | `TV_654321`                | ―                                     |
| `region`           | enum   | `Ghent`                    | Extendable                            |
| `deviceType`       | enum   | `SmartTV`                  | ―                                     |
| `channelId`        | string | `HBO`                      | Only for `channel_*` events           |
| `watchDurationSec` | int    | `1800`                     | Only for `channel_end`                |
| …                  | …      | …                          | See `data_generator.py` for full list |

---

## 🚀 Quick Start

1. **Clone & create virtual env**

   ```bash
   git clone <your‑repo‑url>
   cd tv‑streaming‑demo
   python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
   ```

2. **Install Python deps & start local Kafka**

   ```bash
   pip install kafka-python pyspark==3.5.1 faker
   # If you don't already have Kafka:
   curl -LO https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz && \
   tar xzf kafka_2.13-3.7.0.tgz && cd kafka_*/
   bin/zookeeper-server-start.sh config/zookeeper.properties &
   bin/kafka-server-start.sh     config/server.properties &
   ```

3. **Launch the full pipeline**

   ```bash
   python main.py  --bootstrap localhost:9092  --topic tv_events  --rate 2  # events/sec
   ```

   This does three things:

   * Spins up the in‑process **event generator**.
   * Streams events into `tv_events` via **kafka\_producer.py**.
   * Starts **spark\_consumer.py** which reads the topic and writes daily Parquet files to `./warehouse/tv_events/date=YYYY-MM-DD/`.

4. **Inspect the results**

   ```bash
   spark-sql -S "SELECT event, COUNT(*) FROM parquet.`warehouse/tv_events/*` GROUP BY event ORDER BY 2 DESC LIMIT 5"
   ```

   Or point your BI tool / notebook at the Parquet dir.

---

## 🔍 Component Details

### `data_generator.py`

* Uses **Faker** + custom helper functions.
* Guarantees logical consistency (e.g., `channel_end` must match a preceding `channel_start`).
* Exposed via `generate_event()`—returns a plain dict.

### `kafka_producer.py`

* Wraps **kafka‑python**.
* Command‑line flags for broker list, topic, and throughput.

### `spark_consumer.py`

* Spark Structured Streaming (PySpark 3.5+).
* Parses the JSON payload, applies basic schema, and writes to partitioned Parquet.
* Fault‑tolerant via checkpointing to `./chk/tv_events/`.

---

## ☕ Running Pieces Individually

```bash
# Just view a handful of sample events
python -c "import data_generator, json, itertools; \
for e in itertools.islice(map(data_generator.generate_event, itertools.repeat(None)), 5): 
    print(json.dumps(e, indent=2))"

# Produce indefinitely without Spark consumer
python kafka_producer.py --topic tv_events

# Later, attach Spark only
python spark_consumer.py --topic tv_events
```

---

## 📈 Extending the Demo

| Idea                                   | Where to tweak                                     |
| -------------------------------------- | -------------------------------------------------- |
| Add new event types                    | `EVENT_TYPES` list in **data\_generator.py**       |
| Push to a cloud Kafka (MSK, Confluent) | Flags in **kafka\_producer.py**                    |
| Write to Delta Lake or Iceberg         | Replace `write.format("parquet")` in consumer      |
| Show near‑real‑time dashboard          | Point **Apache Superset** or **Grafana** at output |

Pull requests welcome—let’s make this the best tiny OTT pipeline on GitHub! 🎬🍿

---

## 📜 License

[MIT](LICENSE)
