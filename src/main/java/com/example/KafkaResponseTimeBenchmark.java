package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;


import java.util.concurrent.ExecutionException;

public class KafkaResponseTimeBenchmark {
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String TOPIC = "test-topic";
    private static final int NUM_RUNS = 1000;
    private static final int MSGS_PER_RUN = 1000;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // ============ PRODUCER TEST ============
        System.out.printf("📤 PRODUCER TEST: %d runs x %d messages each run%n", NUM_RUNS, MSGS_PER_RUN);
        System.out.println("=".repeat(60));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        List<Long> produceTimes = new ArrayList<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int run = 0; run < NUM_RUNS; run++) {
                long startRun = System.currentTimeMillis();

                // Send 1000 messages in this run
                List<ProducerRecord<String, String>> records = new ArrayList<>();
                for (int i = 0; i < MSGS_PER_RUN; i++) {
                    records.add(new ProducerRecord<>(TOPIC, "Test message"));
                }

                // Send all messages and wait for acknowledgments
                for (ProducerRecord<String, String> record : records) {
                    producer.send(record).get();
                }

                long endRun = System.currentTimeMillis();
                produceTimes.add(endRun - startRun);

                if ((run + 1) % 100 == 0) {
                    System.out.printf("   Completed %d/%d runs%n", run + 1, NUM_RUNS);
                }
            }
        }

        double produceMedian = calculateMedian(produceTimes);
        System.out.printf("%n✅ Producer median response time (per 1000 messages): %.2f ms%n", produceMedian);
        System.out.println();

        // ============ PREPARE FOR CONSUMER TEST ============
        System.out.println("🗑️ PREPARING for consumer test: Loading 1M messages...");
        System.out.println("=".repeat(60));

        // Delete and recreate topic
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            try {
                admin.deleteTopics(Collections.singletonList(TOPIC)).all().get();
                Thread.sleep(2000);
            } catch (ExecutionException e) {
                // Topic might not exist, continue
            }

            NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);
            try {
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }
            Thread.sleep(1000);
        }

        // Produce 1,000,000 messages (1000 runs x 1000 messages) for consumer test
        Properties producerProps2 = new Properties();
        producerProps2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        producerProps2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps2)) {
            for (int run = 0; run < NUM_RUNS; run++) {
                for (int i = 0; i < MSGS_PER_RUN; i++) {
                    producer.send(new ProducerRecord<>(TOPIC, "Fresh message"));
                }
                if ((run + 1) % 100 == 0) {
                    System.out.printf("   Produced %d/%d runs (%,d messages)%n", 
                        run + 1, NUM_RUNS, (run + 1) * MSGS_PER_RUN);
                }
            }
            producer.flush();
        }

        System.out.printf("✅ Queue now has %d messages%n", NUM_RUNS * MSGS_PER_RUN);
        System.out.println();

        // ============ CONSUMER TEST ============
        System.out.printf("📥 CONSUMER TEST: %d runs x %d messages each run%n", NUM_RUNS, MSGS_PER_RUN);
        System.out.println("=".repeat(60));

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "benchmark-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        List<Long> consumeTimes = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            for (int run = 0; run < NUM_RUNS; run++) {
                long startRun = System.currentTimeMillis();

                // Consume 1000 messages in this run
                int messagesReceived = 0;
                while (messagesReceived < MSGS_PER_RUN) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                    if (records.isEmpty()) {
                        break;
                    }
                    messagesReceived += records.count();
                }

                long endRun = System.currentTimeMillis();

                if (messagesReceived == MSGS_PER_RUN) {
                    consumeTimes.add(endRun - startRun);
                } else {
                    System.out.printf("   ⚠️ Run %d: Only received %d/%d messages%n", 
                        run + 1, messagesReceived, MSGS_PER_RUN);
                    break;
                }

                if ((run + 1) % 100 == 0) {
                    System.out.printf("   Completed %d/%d runs%n", run + 1, NUM_RUNS);
                }
            }
        }

        double consumeMedian = calculateMedian(consumeTimes);
        System.out.printf("%n✅ Consumer median response time (per 1000 messages): %.2f ms%n", consumeMedian);
        System.out.println();

        // ============ FINAL RESULTS ============
        System.out.println("=".repeat(70));
        System.out.println("FINAL RESULTS");
        System.out.println("=".repeat(70));
        System.out.printf("Producer: %d runs of %d messages each%n", NUM_RUNS, MSGS_PER_RUN);
        System.out.printf("├─ Median response time per 1000 messages: %.2f ms%n", produceMedian);
        System.out.printf("└─ Total messages sent: %,d%n", NUM_RUNS * MSGS_PER_RUN);
        System.out.println();
        System.out.printf("Consumer: %d runs of %d messages each%n", NUM_RUNS, MSGS_PER_RUN);
        System.out.printf("├─ Median response time per 1000 messages: %.2f ms%n", consumeMedian);
        System.out.printf("└─ Total messages consumed: %,d%n", NUM_RUNS * MSGS_PER_RUN);
        System.out.println("=".repeat(70));
    }

    private static double calculateMedian(List<Long> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        Collections.sort(values);
        int size = values.size();
        if (size % 2 == 0) {
            return (values.get(size / 2 - 1) + values.get(size / 2)) / 2.0;
        } else {
            return values.get(size / 2);
        }
    }
}