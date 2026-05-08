# Kafka & JMS Benchmark Report


> Kafka vs JMS Non-Persistent vs JMS Persistent

**Date:** 2026-05-04
**Setup:** Kafka 2.13-4.1.1 / 4.2.0 · `localhost:9092` · 1 partition · RF=1 · 1 KB message payload

---

## 00 · Key Numbers

| Metric | Value |
|---|---|
| Stable producer ceiling | **10K msg/s** (sub-5ms p50) |
| Hardware ceiling | **173K msg/s** / 165 MB/s |
| Consumer peak (perf tool) | **645K msg/s** / 615 MB/s |
| Consumer benchmark final | **204K msg/s** (after JIT warmup) |
| Response time — producer | **79 ms** median / 1K msgs (acks=all) |
| Response time — consumer | **2 ms** median / 1K msgs |
| p99 at breakpoint | **808 ms** (at 100K msg/s target) |
| E2E median latency | **2,015 ms** (async test — harness artifact) |

---

## 01 · Producer Throughput Test (`kafka-producer-perf-test`)

| Round | Target | Actual rate | MB/s | Avg lat | Max lat | p50 | p95 | p99 | p99.9 | Status |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000 msg/s | 999.9/s | 0.95 | 3.45 ms | 476 ms | 3 ms | 6 ms | 8 ms | 23 ms | [PASS] Stable |
| 2 | 10,000 msg/s | 9,993/s | 9.53 | 3.55 ms | 332 ms | 1 ms | 11 ms | 75 ms | 85 ms | [PASS] Stable |
| 3 | 100,000 msg/s | 107,330/s | 102.4 | 203.9 ms | 881 ms | 194 ms | 340 ms | 808 ms | 879 ms | [WARN] Saturated |
| 4 | Unlimited | 172,640/s | 164.6 | 24.6 ms | 467 ms | 1 ms | 150 ms | 342 ms | 446 ms | [WARN] Ceiling |

> **Note:** Breakpoint between 10K → 100K msg/s. Hardware ceiling ~173K msg/s (unlimited run). Round 3 forces unnatural pacing — the rate limiter queues messages internally, inflating p99 to 808ms. Round 4 lets Kafka batch optimally, explaining the lower average latency at higher actual throughput.

---

## 02 · Response Time Test (`KafkaResponseTimeBenchmark`)

1,000 runs x 1,000 messages each.

| Side | Median response time | Notes |
|---|---|---|
| **Producer** | **79 ms** | Synchronous sends with `acks=all` — each send waits for broker + all replicas |
| **Consumer** | **2 ms** | Batch polling from page cache — ~40x faster than synchronous produce |

---

## 03 · End-to-End Latency (`KafkaLatencyTest` — async)

10,000 messages tested.

| Stat | Value |
|---|---|
| Min | 1,907 ms |
| **Median** | **2,015 ms** <- key metric |
| Mean | 2,011 ms |
| P99 | 2,143 ms |
| Max | 2,255 ms |

> **Note:** The ~2s floor is a **test harness artifact**, not Kafka's true latency. The async test sleeps 1s before first poll, and all 10K messages are produced before the consumer catches up.

---

## 04 · Producer Benchmark (`KafkaProducerBenchmark` — exponential sweep)

| Target | Actual msg/s | MB/s | Duration | Status |
|---|---|---|---|---|
| 100 msg/s | 3,333 | 3.26 | 30 ms | OK |
| 200 msg/s | 7,407 | 7.24 | 27 ms | OK |
| 400 msg/s | 44,444 | 43.45 | 9 ms | OK |
| 800 msg/s | 42,105 | 41.16 | 19 ms | OK |
| 1,600 msg/s | 76,190 | 74.48 | 21 ms | OK |
| 3,200 msg/s | 133,333 | 130.34 | 24 ms | OK |
| 6,400 msg/s | 152,381 | 148.95 | 42 ms | OK |
| 12,800 msg/s | 166,234 | 162.50 | 77 ms | OK |
| 25,600 msg/s | 107,113 | 104.70 | 239 ms | OK |
| 51,200 msg/s | 141,047 | 137.88 | 363 ms | OK |
| 102,400 msg/s | 144,633 | 141.38 | 708 ms | OK |
| 204,800 msg/s | 231,151 | 225.95 | 886 ms | OK |
| 409,600 msg/s | 256,000 | 250.24 | 1,600 ms | OK |
| 819,200 msg/s | 286,133 | 279.70 | 2,863 ms | OK |
| **1,638,400 msg/s** | **369,759** | **361.44** | 4,431 ms | PEAK |
| 3,276,800 msg/s | 277,648 | 271.41 | 11,802 ms | DECLINING |
| 6,553,600 msg/s | 247,811 | 242.24 | 26,446 ms | DECLINING |

> Peak actual throughput ~370K msg/s at the 1.6M target. All runs completed with 0 failures.

---

## 05 · Consumer Benchmark (`KafkaConsumerBenchmark`)

| Metric | Value |
|---|---|
| Total consumed | 4.1M messages |
| Total duration | 20.1 s |
| Starting throughput | 13K msg/s (cold) |
| **Final throughput** | **204K msg/s** |

Throughput ramp is textbook JVM JIT warm-up. Key config: `FETCH_MIN_BYTES=1MB`, `MAX_POLL_RECORDS=5000`.

---

## 06 · Consumer Group Comparison (`throughput_test.sh`)

| Group | Messages | Peak msg/s | Peak MB/s | Rebalance ms | Status |
|---|---|---|---|---|---|
| consumer-group-small | 100K | ~0.3/s | ~0.0003 | 3,357–3,492 | [FAIL] Race condition |
| **consumer-group-large** | 1M | **644,596/s** | 614.7 | 3,382 | [PASS] Success |
| consumer-group-large-fetch (1 MB fetch) | 1M | 350,416/s | 334.2 | 3,374 | [WARN] Partial read |

---

## 07 · Configuration & Tuning

### Producer

```properties
# Response time test
acks=all          # full durability
linger.ms=0       # no batching delay

# Throughput benchmark
acks=1            # leader-only ack
linger.ms=5
batch.size=65536  # 64 KB

# perf test
record.size=1000  # 1 KB
throughput=-1     # unlimited (Round 4)
```

### Consumer (`KafkaConsumerBenchmark`)

```properties
auto.offset.reset=earliest
enable.auto.commit=false
fetch.min.bytes=1048576   # 1 MB
fetch.max.wait.ms=500
fetch.max.bytes=52428800  # 50 MB
max.poll.records=5000
# commit.interval = 10,000 msgs (async); final commit = sync
```

---

## 08 · JMS Lab 4 Performance Metrics (Non-Persistent)

### Key Metrics

| Metric | Value |
|---|---|
| Producer response time | **6 ms** median / 1,000 messages |
| Consumer response time | **6 ms** median / 1,000 messages |
| Max tested throughput | **128K msg/s** (all succeeded) |
| Median E2E latency | **3.995 ms** / 10,000 messages |

### Producer vs Consumer Response Time vs Kafka

| | JMS Non-Persistent | Kafka |
|---|---|---|
| Producer | **6 ms** (~13x faster) | 79 ms |
| Consumer | 6 ms | **2 ms** (~3x faster) |

### Producer Throughput (Rate-Based)

| Target rate | Sent | Received | Time (s) | Actual msg/s | Status |
|---|---|---|---|---|---|
| 500 msg/s | 500 | 500 | 0.81 | 617 | PASS |
| 1,000 msg/s | 1,000 | 1,000 | 0.81 | 1,234 | PASS |
| 2,000 msg/s | 2,000 | 2,000 | 0.83 | 2,409 | PASS |
| 4,000 msg/s | 4,000 | 4,000 | 0.85 | 4,706 | PASS |
| 8,000 msg/s | 8,000 | 8,000 | 0.85 | 9,411 | PASS |
| 16,000 msg/s | 16,000 | 16,000 | 0.89 | 17,977 | PASS |
| 32,000 msg/s | 32,000 | 32,000 | 0.98 | 32,653 | PASS |
| 64,000 msg/s | 64,000 | 64,000 | 1.18 | 54,237 | PASS |
| **128,000 msg/s** | 128,000 | 128,000 | 1.56 | 82,051 | PASS |

### Consumer Throughput (Rate-Based)

Same tiers as producer — all 9 rates succeeded with 0 message loss. Consumer time at 128K target: 1.38s (actual 92,753 msg/s).

### JMS Non-Persistent vs Kafka — Side-by-Side

| Metric | JMS Non-Persistent | Kafka | Winner |
|---|---|---|---|
| Producer response time | **6 ms** | 79 ms | JMS (~13x) |
| Consumer response time | 6 ms | **2 ms** | Kafka (3x) |
| E2E median latency | **3.995 ms** | 2,015 ms* | JMS (~504x) |
| Max tested throughput | 128K msg/s | **173K msg/s** | Kafka (1.35x) |
| Consumer peak (perf tool) | — | **645K msg/s** | Kafka |
| Message loss at max rate | 0 | 0 | Tie |
| Durability (broker restart) | No — Lost | Yes — Durable | Kafka |

*Kafka E2E figure is a harness artifact (+1s sleep). True Kafka broker latency is sub-10ms at low rates.

---

## 09 · JMS Persistent — JMeter Load Test

### JMeter Configuration

```
Ramp-up time:    1 sec
Delivery mode:   PERSISTENT (disk-flushed per message)
Transport:       ActiveMQ / JMS queue
Message payload: 1 KB
```

### Key Metrics

| Metric | Value |
|---|---|
| Produce response time | **2,288 ms** median / 1,000 messages |
| Consume response time | **0 ms** median (cached) |
| Producer ceiling | ~1,003 msg/s (pass) |
| Producer at 2K samples | FAIL — 50% error rate |
| Consumer ceiling | ~977 msg/s (2K pass) |
| Consumer at 4K samples | FAIL |

### Producer Throughput

| Samples | Throughput | Error % | Status |
|---|---|---|---|
| 1,000 | 1,003 msg/s | 0% | PASS |
| 2,000 | 1,000 msg/s | 50% | FAIL |

### Consumer Throughput

| Samples | Throughput | Error % | Status |
|---|---|---|---|
| 1,000 | 977 msg/s | 0% | PASS |
| 2,000 | 977 msg/s | 0% | PASS |
| 4,000 | — | — | FAIL |

### Produce Response Time: Persistent vs Non-Persistent JMS vs Kafka

| System | Produce latency |
|---|---|
| JMS Non-Persistent | **6 ms** |
| Kafka (acks=all) | 79 ms |
| **JMS Persistent** | **2,288 ms** |

> JMS Persistent is ~381x slower than JMS Non-Persistent and ~29x slower than Kafka. Each message requires a synchronous disk flush before acknowledgment.

---

## 10 · Three-Way Comparison — Kafka vs JMS Non-Persistent vs JMS Persistent

| Metric | JMS Non-Persistent | Kafka | JMS Persistent | Winner |
|---|---|---|---|---|
| Produce response time (1K msgs) | **6 ms** | 79 ms | 2,288 ms | JMS NP |
| Consume response time | 6 ms | **2 ms** | 0 ms* | Kafka |
| E2E median latency (10K msgs) | **3.995 ms** | 2,015 ms† | ~2,288 ms‡ | JMS NP |
| Max raw throughput | 128K msg/s (tested) | **173K msg/s** | <2K msg/s | Kafka |
| Consumer peak (perf tool) | — | **645K msg/s** | — | Kafka |
| Durability | No | Yes — Full (RF=1) | Yes — Per-message fsync | Kafka / JMS P |
| Message replay / rewind | No | Yes — Offset seek | No — Consumed = gone | Kafka |
| Consumer group fan-out | No — Single | Yes — Unlimited groups | No — Single | Kafka |
| Operational complexity | Low | High (KRaft, topics) | Low–Medium | JMS NP |
| Message loss at max rate | 0 | 0 | 50% at 2K samples | JMS NP / Kafka |

*Already in memory when polled. †Harness artifact. ‡Dominated by produce write path.

---

## When to Choose Each

### JMS Non-Persistent
- [+] Ultra-low latency needed (<10ms)
- [+] Messages can be lost on restart
- [+] Simple pub-sub, low volume
- [+] Internal service orchestration
- [-] No replay, no durability, no fan-out

### Kafka
- [+] High throughput (>10K msg/s)
- [+] Durable, replayable log
- [+] Multiple consumer groups
- [+] Event streaming pipelines
- [-] Higher operational overhead

### JMS Persistent
- [+] Per-message durability required
- [+] Low-to-moderate volume (<1K msg/s)
- [+] Financial / transactional messages
- [+] Existing JMS ecosystem
- [-] Bottleneck above ~1K msg/s

---

*Kafka & JMS Benchmark Report · Lab 4 · CSED DDIA · Kafka 2.13-4.x · JMS/ActiveMQ · localhost:9092 · 1 partition · RF=1*
