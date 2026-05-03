"""
Kafka Producer Benchmark (Python)
Mirrors the logic of KafkaProducerBenchmarkReal.java
Requires: pip install kafka-python psutil
"""

import threading
import time
import statistics
import psutil
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "benchmark-topic"
RESPONSE_RUNS = 1000

MEMORY_FLOOR_MB = 500
JVM_HEAP_FLOOR_MB = 50          # not applicable in Python; kept for parity

THROUGHPUT_START = 100
THROUGHPUT_CEILING = 10_000_000

# Shared abort flag
abort = threading.Event()


# ── Payload loader ─────────────────────────────────────────────────────────────
def load_payload(path: str) -> bytes:
    text = Path(path).read_text(encoding="utf-8")
    payload = text.encode("utf-8")
    print(f"Payload size: {len(payload)} bytes")
    return payload


# ── Memory watchdog ────────────────────────────────────────────────────────────
def memory_watchdog():
    """Polls available system memory every 500 ms and sets abort if low."""
    while not threading.current_thread()._stop_event.wait(0.5):
        mem = psutil.virtual_memory()
        free_mb = mem.available / (1024 * 1024)

        if free_mb < MEMORY_FLOOR_MB:
            abort.set()
            print("[WATCHDOG] Memory low → aborting")
            return


def start_memory_watchdog() -> threading.Thread:
    t = threading.Thread(target=memory_watchdog, daemon=True)
    t._stop_event = threading.Event()
    t.start()
    return t


# ── Stats printer ──────────────────────────────────────────────────────────────
def print_stats(values: list[int]):
    values_sorted = sorted(values)
    n = len(values_sorted)
    median = values_sorted[n // 2]
    minimum = values_sorted[0]
    maximum = values_sorted[-1]
    p95 = values_sorted[int(n * 0.95)]
    print(f"Median: {median} ms | Min: {minimum} | Max: {maximum} | P95: {p95}")


# ── Base producer factory ──────────────────────────────────────────────────────
def make_producer(**kwargs) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=None,
        value_serializer=None,
        **kwargs,
    )


# ── Response-time benchmark ────────────────────────────────────────────────────
def benchmark_response_time(payload: bytes):
    print("\n=== Response Time ===")

    producer = make_producer(
        acks=1,
        linger_ms=0,
    )

    try:
        # Warm-up
        for _ in range(50):
            producer.send(TOPIC, value=payload).get(timeout=30)

        latencies: list[int] = []

        for i in range(RESPONSE_RUNS):
            if abort.is_set():
                break
            start = time.perf_counter()
            producer.send(TOPIC, value=payload).get(timeout=30)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            latencies.append(elapsed_ms)

        print_stats(latencies)

    finally:
        producer.close()


# ── Single load-test run ───────────────────────────────────────────────────────
def run_load_test(producer: KafkaProducer, rate: int, payload: bytes) -> bool:
    errors = threading.local()
    error_count = [0]           # mutable container for callback closure
    completed = [0]
    lock = threading.Lock()
    done_event = threading.Event()

    def on_send_success(_):
        with lock:
            completed[0] += 1
            if completed[0] == rate:
                done_event.set()

    def on_send_error(_record_metadata, exc):
        with lock:
            error_count[0] += 1
            completed[0] += 1
            if completed[0] == rate:
                done_event.set()

    start_ns = time.perf_counter_ns()

    for _ in range(rate):
        if abort.is_set():
            break
        producer.send(TOPIC, value=payload).add_callback(on_send_success).add_errback(on_send_error)

    done_event.wait(timeout=30)
    producer.flush()

    duration_ms = (time.perf_counter_ns() - start_ns) // 1_000_000
    failed = error_count[0]
    actual_rate = (rate - failed) / (duration_ms / 1000) if duration_ms > 0 else 0

    print(f"number of messages sent: {rate}")
    print(f"number of messages failed: {failed}")
    print(f"   duration: {duration_ms} ms | failed: {failed} | actual: {actual_rate:.2f} msg/s")

    return failed == 0 and not abort.is_set()


# ── Throughput benchmark ───────────────────────────────────────────────────────
def benchmark_throughput(payload: bytes):
    print("\n=== Throughput ===")

    producer = make_producer(
        acks=1,
        linger_ms=5,
        batch_size=65536,
    )

    try:
        last_good = 0
        rate = THROUGHPUT_START

        while rate <= THROUGHPUT_CEILING and not abort.is_set():
            ok = run_load_test(producer, rate, payload)
            print(f"Rate {rate} msg/s → {'OK' if ok else 'FAILED'}")

            if not ok:
                print(f"Max throughput: {last_good} msg/s")
                return

            last_good = rate
            rate *= 2

    finally:
        producer.close()


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    payload = load_payload("message.txt")

    watchdog = start_memory_watchdog()

    try:
        benchmark_response_time(payload)
        if not abort.is_set():
            benchmark_throughput(payload)
    finally:
        watchdog._stop_event.set()


if __name__ == "__main__":
    main()