import time
import statistics
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json

# Configuration
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'latency-test-topic'
NUM_MESSAGES = 10000

def create_topic():
    """Create test topic"""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    try:
        admin.delete_topics([TOPIC])
        time.sleep(2)
    except:
        pass
    
    admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
    time.sleep(1)
    admin.close()

def start_consumer():
    """Start consumer that will consume messages as they arrive"""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='latest',  # Start from newest messages
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    return consumer

def produce_messages_with_timestamp():
    """Producer that attaches timestamp to each message"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        acks='all',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"📤 Producing {NUM_MESSAGES} messages with timestamps...")
    
    for i in range(NUM_MESSAGES):
        # Create message with send timestamp (milliseconds)
        message = {
            'message_id': i,
            'send_timestamp': time.time() * 1000,  # Current time in milliseconds
            'content': f'Test message {i}'
        }
        
        future = producer.send(TOPIC, value=message)
        future.get()  # Wait for acknowledgment
        
        if (i + 1) % 1000 == 0:
            print(f"   Produced {i + 1}/{NUM_MESSAGES} messages")
    
    producer.flush()
    producer.close()
    print(f"✅ Produced {NUM_MESSAGES} messages\n")

def consume_and_calculate_latency():
    """Consumer that calculates latency for each message"""
    print(f"📥 Consuming {NUM_MESSAGES} messages and calculating latency...")
    
    consumer = start_consumer()
    
    latencies = []
    messages_consumed = 0
    
    # Create a consumer that actively consumes messages
    # We need to ensure the consumer is running while producer is sending
    # For accurate latency measurement, we'll run consumer in same thread
    # and measure from send to receive
    
    # Alternative approach: Use a single consumer that polls continuously
    timeout_ms = 60000  # 60 seconds timeout
    start_time = time.time()
    
    while messages_consumed < NUM_MESSAGES and (time.time() - start_time) < 60:
        records = consumer.poll(timeout_ms=1000, max_records=100)
        
        for tp, messages in records.items():
            for msg in messages:
                # Decode message
                message_data = json.loads(msg.value.decode('utf-8'))
                
                # Get send timestamp from message
                send_timestamp = message_data['send_timestamp']
                
                # Get current timestamp (receive time)
                receive_timestamp = time.time() * 1000
                
                # Calculate latency in milliseconds
                latency = receive_timestamp - send_timestamp
                latencies.append(latency)
                messages_consumed += 1
                
                if messages_consumed % 1000 == 0:
                    print(f"   Consumed {messages_consumed}/{NUM_MESSAGES} messages")
    
    consumer.close()
    
    if len(latencies) == NUM_MESSAGES:
        median_latency = statistics.median(latencies)
        mean_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        
        print(f"\n✅ Successfully calculated latency for {NUM_MESSAGES} messages")
        return {
            'latencies': latencies,
            'median': median_latency,
            'mean': mean_latency,
            'min': min_latency,
            'max': max_latency,
            'p99': p99_latency
        }
    else:
        print(f"⚠️ Only received {messages_consumed}/{NUM_MESSAGES} messages")
        return None

def run_latency_test_with_consumer_first():
    """
    Better approach: Start consumer first, then produce messages
    This ensures consumer is actively waiting for messages
    """
    print("=" * 70)
    print("KAFKA MEDIAN LATENCY TEST")
    print("=" * 70)
    
    # Setup
    create_topic()
    
    # Approach 1: Consumer polls while producer runs
    print("\n🔄 Starting latency measurement...")
    print("   (Consumer actively waiting for messages)")
    
    consumer = start_consumer()
    
    # Create producer and consumer in same thread for accurate measurement
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        acks='all',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    latencies = []
    
    print(f"\n📊 Running test with {NUM_MESSAGES} messages...\n")
    
    for i in range(NUM_MESSAGES):
        # Send message with timestamp
        send_time = time.time() * 1000
        message = {
            'message_id': i,
            'send_timestamp': send_time,
            'content': f'Test message {i}'
        }
        
        # Produce message
        producer.send(TOPIC, value=message)
        
        # Immediately try to consume (for low latency measurement)
        # In real scenario, consumer would be running continuously
        # We'll poll with short timeout
        records = consumer.poll(timeout_ms=100, max_records=1)
        
        # Calculate receive time
        receive_time = time.time() * 1000
        
        if records:
            for tp, messages in records.items():
                for msg in messages:
                    # Parse message to get send timestamp
                    msg_data = json.loads(msg.value.decode('utf-8'))
                    actual_send_time = msg_data['send_timestamp']
                    latency = receive_time - actual_send_time
                    latencies.append(latency)
                    
                    if (i + 1) % 1000 == 0:
                        print(f"   Processed {i + 1}/{NUM_MESSAGES} messages - Current latency: {latency:.2f} ms")
    
    producer.flush()
    producer.close()
    consumer.close()
    
    # Calculate statistics
    if latencies:
        median_latency = statistics.median(latencies)
        mean_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        
        print("\n" + "=" * 70)
        print("LATENCY RESULTS")
        print("=" * 70)
        print(f"Total messages tested: {len(latencies):,}")
        print(f"\n📊 Latency Statistics (milliseconds):")
        print(f"   Min Latency:  {min_latency:.2f} ms")
        print(f"   Median Latency: {median_latency:.2f} ms  ← KEY METRIC")
        print(f"   Mean Latency:  {mean_latency:.2f} ms")
        print(f"   P99 Latency:   {p99_latency:.2f} ms")
        print(f"   Max Latency:   {max_latency:.2f} ms")
        print("=" * 70)
        
        return median_latency
    else:
        print("❌ No messages were consumed")
        return None

# Alternative: Async producer and consumer
def run_async_latency_test():
    """Run producer and consumer in separate threads for more realistic scenario"""
    import threading
    from queue import Queue
    
    print("\n🔄 Running async latency test (producer & consumer in parallel)...")
    
    create_topic()
    
    # Queue to collect latencies
    latency_queue = Queue()
    
    # Consumer thread
    def consumer_worker():
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000
        )
        
        messages_consumed = 0
        while messages_consumed < NUM_MESSAGES:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            for tp, messages in records.items():
                for msg in messages:
                    msg_data = json.loads(msg.value.decode('utf-8'))
                    send_time = msg_data['send_timestamp']
                    receive_time = time.time() * 1000
                    latency = receive_time - send_time
                    latency_queue.put(latency)
                    messages_consumed += 1
            
            if messages_consumed % 1000 == 0:
                print(f"   Consumer: {messages_consumed}/{NUM_MESSAGES} messages")
        
        consumer.close()
    
    # Start consumer thread
    consumer_thread = threading.Thread(target=consumer_worker)
    consumer_thread.start()
    
    # Wait a bit for consumer to start
    time.sleep(1)
    
    # Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        acks='all',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"\n📤 Producing {NUM_MESSAGES} messages...")
    for i in range(NUM_MESSAGES):
        message = {
            'message_id': i,
            'send_timestamp': time.time() * 1000,
            'content': f'Test message {i}'
        }
        producer.send(TOPIC, value=message)
        
        if (i + 1) % 1000 == 0:
            print(f"   Producer: {i + 1}/{NUM_MESSAGES} messages")
    
    producer.flush()
    producer.close()
    
    # Wait for consumer to finish
    consumer_thread.join()
    
    # Collect latencies
    latencies = []
    while not latency_queue.empty():
        latencies.append(latency_queue.get())
    
    if latencies:
        median_latency = statistics.median(latencies)
        print(f"\n✅ Median Latency: {median_latency:.2f} ms")
        return median_latency
    else:
        return None

if __name__ == "__main__":
    # Run the latency test
    print("KAFKA LATENCY TEST")
    print("Measures the delay between message send and receive\n")
    
    # Choose which method to use
    # Method 1: Sequential (simpler)
    # median = run_latency_test_with_consumer_first()
    
    # Method 2: Parallel threads (more realistic)
    median = run_async_latency_test()
    
    print("\n✅ Test completed!")