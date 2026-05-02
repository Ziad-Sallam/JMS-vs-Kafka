package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class LatencyTest {

    static final String TOPIC     = "latency-topic";
    static final String BOOTSTRAP = "localhost:9092";
    static final int MESSAGES     = 10_000;
    static final List<Long> latencies =
        Collections.synchronizedList(new ArrayList<>());
    static volatile boolean producerDone = false;

    public static void main(String[] args) throws Exception {

        System.out.println("=== Kafka Median Latency Test ===");
        System.out.println("Sending and receiving " + MESSAGES + " messages...");
 
        Thread consumerThread = new Thread(LatencyTest::consume);
        consumerThread.setDaemon(false);
        consumerThread.start();

        Thread.sleep(3000);    //    wait for consumer to be ready

        produce();

        consumerThread.join(60_000);

        if (latencies.size() > 0) {
            Collections.sort(latencies);
            int mid = latencies.size() / 2;
            System.out.println("\n=== Results ===");
            System.out.println("Messages measured : " + latencies.size());
            System.out.println("Median Latency    : " + latencies.get(mid) + " ms");
            System.out.println("Min Latency       : " + latencies.get(0) + " ms");
            System.out.println("Max Latency       : " + latencies.get(latencies.size()-1) + " ms");
        } else {
            System.out.println("No messages received!");
        }
    }

    static void produce() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < MESSAGES; i++) {
            // embed send time in the msg for latency measurement
            String value = System.currentTimeMillis() + "|" + "A".repeat(1000);
            producer.send(new ProducerRecord<>(TOPIC, value));
        }
        producer.flush();
        producer.close();
        producerDone = true;
        System.out.println("Producer done. Waiting for consumer...");
    }

    static void consume() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("group.id", "latency-group-" + System.currentTimeMillis());
        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        int count = 0;
        int emptyPolls = 0;

        while (count < MESSAGES && emptyPolls < 10) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(2000));

            if (records.isEmpty()) {
                emptyPolls++;
                continue;
            }
            emptyPolls = 0;

            for (ConsumerRecord<String, String> record : records) {
                try {
                    long sentTime = Long.parseLong(record.value().split("\\|")[0]);
                    long latency  = System.currentTimeMillis() - sentTime;
                    latencies.add(latency);
                    count++;
                    if (count >= MESSAGES) break;
                } catch (Exception e) {
                    
                }
            }
        }
        consumer.close();
        System.out.println("Consumer done. Received: " + count);
    }
}