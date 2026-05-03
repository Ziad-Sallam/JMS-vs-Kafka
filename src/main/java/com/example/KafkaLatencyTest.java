package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class KafkaLatencyTest {

    // Configuration
    private static final String KAFKA_SERVER   = "localhost:9092";
    private static final String TOPIC          = "latency-test-topic";
    private static final int    NUM_MESSAGES   = 10_000;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // -------------------------------------------------------------------------
    // Topic management
    // -------------------------------------------------------------------------

    private static void createTopic() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            // Delete if exists
            try {
                admin.deleteTopics(List.of(TOPIC)).all().get();
                Thread.sleep(2_000);
            } catch (Exception ignored) {}

            // Create fresh topic
            NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);
            admin.createTopics(List.of(newTopic)).all().get();
            Thread.sleep(1_000);
        }
        System.out.println("✅ Topic created: " + TOPIC);
    }

    // -------------------------------------------------------------------------
    // Producer helpers
    // -------------------------------------------------------------------------

    private static KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_SERVER);
        props.put(ProducerConfig.ACKS_CONFIG,               "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    // -------------------------------------------------------------------------
    // Consumer helpers
    // -------------------------------------------------------------------------

    private static KafkaConsumer<String, String> buildConsumer(String offsetReset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "latency-test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  offsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    // -------------------------------------------------------------------------
    // Helper: build a JSON message string
    // -------------------------------------------------------------------------

    private static String buildMessage(int id, double sendTimestamp) {
        try {
            Map<String, Object> msg = new LinkedHashMap<>();
            msg.put("message_id",      id);
            msg.put("send_timestamp",  sendTimestamp);
            msg.put("content",         "Test message " + id);
            return MAPPER.writeValueAsString(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // -------------------------------------------------------------------------
    // Statistics
    // -------------------------------------------------------------------------

    private static void printStats(List<Double> latencies) {
        if (latencies.isEmpty()) {
            System.out.println("❌ No latencies collected.");
            return;
        }
        List<Double> sorted = latencies.stream().sorted().collect(Collectors.toList());
        int n = sorted.size();

        double min    = sorted.get(0);
        double max    = sorted.get(n - 1);
        double median = n % 2 == 0
                ? (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0
                : sorted.get(n / 2);
        double mean   = latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double p99    = sorted.get((int) (n * 0.99));

        System.out.println("\n" + "=".repeat(70));
        System.out.println("LATENCY RESULTS");
        System.out.println("=".repeat(70));
        System.out.printf("Total messages tested: %,d%n", n);
        System.out.println("\n📊 Latency Statistics (milliseconds):");
        System.out.printf("   Min Latency:     %.2f ms%n", min);
        System.out.printf("   Median Latency:  %.2f ms  ← KEY METRIC%n", median);
        System.out.printf("   Mean Latency:    %.2f ms%n", mean);
        System.out.printf("   P99 Latency:     %.2f ms%n", p99);
        System.out.printf("   Max Latency:     %.2f ms%n", max);
        System.out.println("=".repeat(70));
    }

    // -------------------------------------------------------------------------
    // Method 1: Sequential (producer → consumer interleaved in same thread)
    // Mirrors run_latency_test_with_consumer_first() from the Python version.
    // -------------------------------------------------------------------------

    public static double runSequentialLatencyTest() throws Exception {
        System.out.println("=".repeat(70));
        System.out.println("KAFKA MEDIAN LATENCY TEST  (Sequential / Same-Thread)");
        System.out.println("=".repeat(70));

        createTopic();

        System.out.println("\n🔄 Starting latency measurement...");
        System.out.println("   (Consumer actively waiting for messages)");

        KafkaConsumer<String, String> consumer = buildConsumer("latest");
        consumer.subscribe(List.of(TOPIC));

        KafkaProducer<String, String> producer = buildProducer();

        List<Double> latencies = new ArrayList<>(NUM_MESSAGES);

        System.out.printf("%n📊 Running test with %,d messages...%n%n", NUM_MESSAGES);

        for (int i = 0; i < NUM_MESSAGES; i++) {
            double sendTime = System.currentTimeMillis();
            String payload  = buildMessage(i, sendTime);

            producer.send(new ProducerRecord<>(TOPIC, payload));

            // Poll with a short timeout – mirrors consumer.poll(timeout_ms=100)
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            double receiveTime = System.currentTimeMillis();

            for (ConsumerRecord<String, String> record : records) {
                @SuppressWarnings("unchecked")
                Map<String, Object> msgData = MAPPER.readValue(record.value(), Map.class);
                double actualSendTime = ((Number) msgData.get("send_timestamp")).doubleValue();
                double latency        = receiveTime - actualSendTime;
                latencies.add(latency);
            }

            if ((i + 1) % 1_000 == 0) {
                System.out.printf("   Processed %,d / %,d messages%n", i + 1, NUM_MESSAGES);
            }
        }

        producer.flush();
        producer.close();
        consumer.close();

        printStats(latencies);

        if (latencies.isEmpty()) return -1;
        List<Double> sorted = latencies.stream().sorted().collect(Collectors.toList());
        int n = sorted.size();
        return n % 2 == 0
                ? (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0
                : sorted.get(n / 2);
    }

    // -------------------------------------------------------------------------
    // Method 2: Async – producer and consumer run in parallel threads.
    // Mirrors run_async_latency_test() from the Python version.
    // -------------------------------------------------------------------------

    public static double runAsyncLatencyTest() throws Exception {
        System.out.println("\n🔄 Running async latency test (producer & consumer in parallel)...");

        createTopic();

        BlockingQueue<Double> latencyQueue = new LinkedBlockingQueue<>();

        // ── Consumer thread ──────────────────────────────────────────────────
        Thread consumerThread = new Thread(() -> {
            KafkaConsumer<String, String> consumer = buildConsumer("earliest");
            consumer.subscribe(List.of(TOPIC));

            int consumed = 0;
            while (consumed < NUM_MESSAGES) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1_000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> msgData =
                                MAPPER.readValue(record.value(), Map.class);
                        double sendTime    = ((Number) msgData.get("send_timestamp")).doubleValue();
                        double receiveTime = System.currentTimeMillis();
                        latencyQueue.put(receiveTime - sendTime);
                        consumed++;

                        if (consumed % 1_000 == 0) {
                            System.out.printf("   Consumer: %,d / %,d messages%n",
                                    consumed, NUM_MESSAGES);
                        }
                    } catch (Exception e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            consumer.close();
        }, "consumer-thread");

        consumerThread.start();

        // Give the consumer a moment to initialize (mirrors time.sleep(1))
        Thread.sleep(1_000);

        // ── Producer (main thread) ────────────────────────────────────────────
        KafkaProducer<String, String> producer = buildProducer();

        System.out.printf("%n📤 Producing %,d messages...%n", NUM_MESSAGES);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            double sendTime = System.currentTimeMillis();
            String payload  = buildMessage(i, sendTime);
            producer.send(new ProducerRecord<>(TOPIC, payload));

            if ((i + 1) % 1_000 == 0) {
                System.out.printf("   Producer: %,d / %,d messages%n", i + 1, NUM_MESSAGES);
            }
        }
        producer.flush();
        producer.close();

        // Wait for consumer to finish
        consumerThread.join();

        // ── Collect results ──────────────────────────────────────────────────
        List<Double> latencies = new ArrayList<>(latencyQueue.size());
        latencyQueue.drainTo(latencies);

        printStats(latencies);

        if (latencies.isEmpty()) return -1;
        List<Double> sorted = latencies.stream().sorted().collect(Collectors.toList());
        int n = sorted.size();
        double median = n % 2 == 0
                ? (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0
                : sorted.get(n / 2);
        System.out.printf("%n✅ Median Latency: %.2f ms%n", median);
        return median;
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        System.out.println("KAFKA LATENCY TEST");
        System.out.println("Measures the delay between message send and receive\n");

        // Uncomment to run the sequential version:
        // runSequentialLatencyTest();

        // Parallel / async version (more realistic):
        runAsyncLatencyTest();

        System.out.println("\n✅ Test completed!");
    }
}