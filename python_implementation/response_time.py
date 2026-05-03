import time
import statistics
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# Configuration
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'test-topic'
NUM_RUNS = 1000
MSGS_PER_RUN = 1000

# ============ PRODUCER TEST ============
print(f"📤 PRODUCER TEST: {NUM_RUNS} runs x {MSGS_PER_RUN} messages each run")
print("=" * 60)

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, acks='all')
produce_times = []

for run in range(NUM_RUNS):
    start_run = time.time() * 1000
    
    # Send 1000 messages in this run
    futures = []
    for i in range(MSGS_PER_RUN):
        future = producer.send(TOPIC, value=b'Test message')
        futures.append(future)
    
    # Wait for all 1000 messages to be acknowledged
    for future in futures:
        future.get()
    
    end_run = time.time() * 1000
    produce_times.append(end_run - start_run)
    
    if (run + 1) % 100 == 0:
        print(f"   Completed {run + 1}/{NUM_RUNS} runs")

producer.close()

produce_median = statistics.median(produce_times)
print(f"\n✅ Producer median response time (per 1000 messages): {produce_median:.2f} ms")
print()

# ============ PREPARE FOR CONSUMER TEST ============
print("🗑️ PREPARING for consumer test: Loading 1M messages...")
print("=" * 60)

# Delete and recreate topic
admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
try:
    admin.delete_topics([TOPIC])
    time.sleep(2)
except:
    pass

admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
time.sleep(1)
admin.close()

# Produce 1,000,000 messages (1000 runs x 1000 messages) for consumer test
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
for run in range(NUM_RUNS):
    for i in range(MSGS_PER_RUN):
        producer.send(TOPIC, value=b'Fresh message')
    if (run + 1) % 100 == 0:
        print(f"   Produced {run + 1}/{NUM_RUNS} runs ({ (run+1)*MSGS_PER_RUN:,} messages)")
producer.flush()
producer.close()

print(f"✅ Queue now has {NUM_RUNS * MSGS_PER_RUN:,} messages")
print()

# ============ CONSUMER TEST ============
print(f"📥 CONSUMER TEST: {NUM_RUNS} runs x {MSGS_PER_RUN} messages each run")
print("=" * 60)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

consume_times = []

for run in range(NUM_RUNS):
    start_run = time.time() * 1000
    
    # Consume 1000 messages in this run
    messages_received = 0
    while messages_received < MSGS_PER_RUN:
        records = consumer.poll(timeout_ms=5000, max_records=MSGS_PER_RUN - messages_received)
        if not records:
            break
        for tp, messages in records.items():
            messages_received += len(messages)
    
    end_run = time.time() * 1000
    
    if messages_received == MSGS_PER_RUN:
        consume_times.append(end_run - start_run)
    else:
        print(f"   ⚠️ Run {run+1}: Only received {messages_received}/{MSGS_PER_RUN} messages")
        break
    
    if (run + 1) % 100 == 0:
        print(f"   Completed {run + 1}/{NUM_RUNS} runs")

consumer.close()

consume_median = statistics.median(consume_times)
print(f"\n✅ Consumer median response time (per 1000 messages): {consume_median:.2f} ms")
print()

# ============ FINAL RESULTS ============
print("=" * 70)
print("FINAL RESULTS")
print("=" * 70)
print(f"Producer: {NUM_RUNS} runs of {MSGS_PER_RUN} messages each")
print(f"├─ Median response time per 1000 messages: {produce_median:.2f} ms")
print(f"└─ Total messages sent: {NUM_RUNS * MSGS_PER_RUN:,}")
print()
print(f"Consumer: {NUM_RUNS} runs of {MSGS_PER_RUN} messages each")
print(f"├─ Median response time per 1000 messages: {consume_median:.2f} ms")
print(f"└─ Total messages consumed: {NUM_RUNS * MSGS_PER_RUN:,}")
print("=" * 70)