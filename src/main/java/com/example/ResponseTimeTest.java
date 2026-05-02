package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class ResponseTimeTest {

    static final String TOPIC   = "producer-consumer-topic";
    static final String BOOTSTRAP = "localhost:9092";
    static final int RUNS = 1000;
    static final String MESSAGE = "A".repeat(1024); // ~1KB

    public static void main(String[] args) throws Exception {
        System.out.println("=== Kafka Response Time Test ===");
        measureProduceTime();
        measureConsumeTime();
    }

    static void measureProduceTime() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        List<Long> times = new ArrayList<>();

        System.out.println("Measuring produce response time over " + RUNS + " runs...");
        for (int i = 0; i < RUNS; i++) {
            long start = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(TOPIC, "key-" + i, MESSAGE)).get(); 
            times.add(System.currentTimeMillis() - start);
        }

        producer.close();
        Collections.sort(times);
        System.out.println("Produce Median Response Time: " + times.get(RUNS / 2) + " ms");
        System.out.println("Produce Min: " + times.get(0) + " ms");
        System.out.println("Produce Max: " + times.get(RUNS - 1) + " ms");
    }

    static void measureConsumeTime() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("group.id", "response-group-" + System.currentTimeMillis());
        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        List<Long> times = new ArrayList<>();
        int count = 0;

        System.out.println("Measuring consume response time over " + RUNS + " runs...");
        while (count < RUNS) {
            long start = System.currentTimeMillis();
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(3000));
            long elapsed = System.currentTimeMillis() - start;
            for (ConsumerRecord<String, String> r : records) {
                times.add(elapsed);
                count++;
                if (count >= RUNS) break;
            }
        }

        consumer.close();
        Collections.sort(times);
        System.out.println("Consume Median Response Time: " + times.get(times.size() / 2) + " ms");
        System.out.println("Consume Min: " + times.get(0) + " ms");
        System.out.println("Consume Max: " + times.get(times.size() - 1) + " ms");
    }
}