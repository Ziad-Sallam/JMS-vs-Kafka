from confluent_kafka import Producer
import time
import struct

BROKER        = 'localhost:9092'
TOPIC         = 'benchmark-topic'
WARMUP_COUNT  = 10000

latencies = []

def delivery_report(err, msg, send_time):
    if err is None:
        latencies.append(time.time() - send_time)
    else:
        print(f"[warn] Delivery error: {err}")

def percentile(lst, p):
    return lst[int(p * len(lst))]

p = Producer({
    'bootstrap.servers':  BROKER,
    'batch.num.messages': 10000,
    'acks':               1,
    "queue.buffering.max.kbytes": 10048576,
})

with open('message.txt', 'rb') as f:
    MESSAGE = f.read()

print(f"Sending {WARMUP_COUNT:,} warm-up messages...")
for _ in range(WARMUP_COUNT):
    p.produce(TOPIC, value=b'warmup')
p.flush()
print("Warm-up done.\n")


for n_messages in [100,1000,10000,100_000, 1_000_000, 1_500_000 ,2_000_000, 2_500_000, 3_000_000, 3_500_000, 4_000_000]:   # test with 1M, 2M, and 5M messages
    print(f"Sending {n_messages:,} benchmark messages...")
    start = time.time()

    for _ in range(n_messages):
        send_time = time.time()
        p.produce(
            TOPIC,
            key=struct.pack('d', send_time),   # 8-byte timestamp in the key
            value=MESSAGE,
            callback=lambda err, msg, t=send_time: delivery_report(err, msg, t)
        )
        p.poll(0)

    p.flush()
    end = time.time()

    total_time = end - start
    throughput  = n_messages / total_time
    latencies.sort()



    print(f"\n{'─'*45}")
    print(f"  Messages sent     : {n_messages:>10,}")
    print(f"  Acks received     : {len(latencies):>10,}")
    print(f"{'─'*45}")
    print(f"  Throughput        : {throughput:>10.2f}  msgs/sec")
    print(f"  Total time        : {total_time:>10.2f}  sec")
    print(f"{'─'*45}")
    print(f"  Avg latency       : {(sum(latencies)/len(latencies))*1000:>10.2f}  ms")
    print(f"  p50  latency      : {percentile(latencies, 0.50)*1000:>10.2f}  ms")
    print(f"  p95  latency      : {percentile(latencies, 0.95)*1000:>10.2f}  ms")
    print(f"  p99  latency      : {percentile(latencies, 0.99)*1000:>10.2f}  ms")
    print(f"  Max  latency      : {latencies[-1]*1000:>10.2f}  ms")
    print(f"{'─'*45}")