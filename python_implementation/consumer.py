from confluent_kafka import Consumer
import time
import struct

BROKER        = 'localhost:9092'
TOPIC         = 'benchmark-topic'
GROUP_ID      = 'benchmark-group-6'
NUM_MESSAGES  = 1000000
WARMUP_COUNT  = 10000
POLL_TIMEOUT  = 10.0

c = Consumer({
    'bootstrap.servers':  BROKER,
    'group.id':           GROUP_ID,
    'auto.offset.reset':  'earliest',
    'enable.auto.commit': False,
})
c.subscribe([TOPIC])

def poll_one():
    while True:
        msg = c.poll(timeout=POLL_TIMEOUT)
        if msg is None:
            return None
        if msg.error():
            print(f"[warn] Kafka error: {msg.error()}")
            continue
        return msg

def percentile(lst, p):
    return lst[int(p * len(lst))]

print(f"Skipping {WARMUP_COUNT:,} warm-up messages...")
for _ in range(WARMUP_COUNT):
    msg = poll_one()
    if msg is None:
        print("[error] Timed out during warm-up.")
        c.close()
        exit(1)
c.commit(asynchronous=False)
print("Warm-up done.\n")

latencies    = []
consumed     = 0
parse_errors = 0

print(f"Consuming {NUM_MESSAGES:,} benchmark messages...")
start = time.time()

while consumed < NUM_MESSAGES:
    msg = poll_one()
    if msg is None:
        print(f"[error] Timed out after {consumed:,} messages.")
        break

    receive_time = time.time()

    try:
        send_time = struct.unpack('d', msg.key())[0]   # unpack 8-byte timestamp
        latencies.append(receive_time - send_time)
    except (struct.error, TypeError):
        parse_errors += 1

    consumed += 1
    c.commit(asynchronous=True)

end = time.time()
c.close()

print(f"\n{'─'*45}")
print(f"  Messages consumed : {consumed:>10,}")
print(f"  Valid latencies   : {len(latencies):>10,}")
print(f"  Parse errors      : {parse_errors:>10,}")
print(f"{'─'*45}")

if not latencies:
    print("[error] No latency data collected.")
else:
    total_time = end - start
    throughput = consumed / total_time
    latencies.sort()

    print(f"  Throughput        : {throughput:>10.2f}  msgs/sec")
    print(f"  Total time        : {total_time:>10.2f}  sec")
    print(f"{'─'*45}")
    print(f"  Avg latency       : {(sum(latencies)/len(latencies))*1000:>10.2f}  ms")
    print(f"  p50  latency      : {percentile(latencies, 0.50)*1000:>10.2f}  ms")
    print(f"  p95  latency      : {percentile(latencies, 0.95)*1000:>10.2f}  ms")
    print(f"  p99  latency      : {percentile(latencies, 0.99)*1000:>10.2f}  ms")
    print(f"  Max  latency      : {latencies[-1]*1000:>10.2f}  ms")
    print(f"{'─'*45}")