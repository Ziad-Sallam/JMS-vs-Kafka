package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerBenchmark {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "benchmark-topic";
    private static final String GROUP_ID = "benchmark-group";

    private static final AtomicBoolean ABORT = new AtomicBoolean(false);

    private static final long MEMORY_FLOOR_MB = 500;
    private static final long JVM_HEAP_FLOOR_MB = 50;

    public static void main(String[] args) {

        Thread watchdog = startMemoryWatchdog();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(baseProps())) {

            consumer.subscribe(Collections.singletonList(TOPIC));

            long totalMessages = 0;
            long start = System.nanoTime();

            while (!ABORT.get()) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) continue;

                totalMessages += records.count();

                consumer.commitSync();

                long elapsedMs = (System.nanoTime() - start) / 1_000_000;

                if (elapsedMs > 0) {
                    double rate = totalMessages / (elapsedMs / 1000.0);

                    System.out.printf(
                            "Consumed: %d msgs | %.2f msg/s%n",
                            totalMessages,
                            rate
                    );
                }

                if (totalMessages >= 4_100_000) break;
            }

            long durationMs = (System.nanoTime() - start) / 1_000_000;

            double finalRate = totalMessages / (durationMs / 1000.0);

            System.out.println("\n=== FINAL RESULT ===");
            System.out.printf("Total messages: %d%n", totalMessages);
            System.out.printf("Duration: %d ms%n", durationMs);
            System.out.printf("Throughput: %.2f msg/s%n", finalRate);

        } finally {
            watchdog.interrupt();
        }
    }

    // ── Consumer config ───────────────────────────────
    private static Properties baseProps() {
        Properties p = new Properties();

        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return p;
    }

    // ── Watchdog ───────────────────────────────
    private static Thread startMemoryWatchdog() {
        Thread t = new Thread(() -> {

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long freeMb = readProcMemAvailable();
                    Runtime rt = Runtime.getRuntime();

                    long jvmMb = (rt.freeMemory()
                            + rt.maxMemory()
                            - rt.totalMemory()) / (1024 * 1024);

                    if (freeMb < MEMORY_FLOOR_MB || jvmMb < JVM_HEAP_FLOOR_MB) {
                        ABORT.set(true);
                        System.out.println("[WATCHDOG] Memory low → aborting consumer");
                        return;
                    }

                    Thread.sleep(500);

                } catch (Exception ignored) {}
            }
        });

        t.setDaemon(true);
        t.start();
        return t;
    }

    private static long readProcMemAvailable() {
        try (Scanner sc = new Scanner(new java.io.File("/proc/meminfo"))) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                if (line.startsWith("MemAvailable:")) {
                    return Long.parseLong(line.split("\\s+")[1]) / 1024;
                }
            }
        } catch (Exception ignored) {}
        return -1;
    }
}