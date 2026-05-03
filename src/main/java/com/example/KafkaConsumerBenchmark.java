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

    // How often to commit (every N messages) and print stats
    private static final int COMMIT_INTERVAL = 10_000;
    private static final int PRINT_INTERVAL  = 50_000;

    public static void main(String[] args) {

        Thread watchdog = startMemoryWatchdog();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(baseProps())) {

            consumer.subscribe(Collections.singletonList(TOPIC));

            long totalMessages   = 0;
            long lastCommitCount = 0;
            long lastPrintCount  = 0;
            long start           = System.nanoTime();

            while (!ABORT.get()) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) continue;

                totalMessages += records.count();

                // ── Async commit every COMMIT_INTERVAL messages ──────────
                // Non-blocking: broker acknowledgement arrives in the background
                // while the next poll is already in flight.
                if (totalMessages - lastCommitCount >= COMMIT_INTERVAL) {
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            System.err.println("[COMMIT ERROR] " + exception.getMessage());
                        }
                    });
                    lastCommitCount = totalMessages;
                }

                // ── Print stats every PRINT_INTERVAL messages ────────────
                if (totalMessages - lastPrintCount >= PRINT_INTERVAL) {
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    if (elapsedMs > 0) {
                        double rate = totalMessages / (elapsedMs / 1000.0);
                        System.out.printf("Consumed: %d msgs | %.2f msg/s%n",
                                totalMessages, rate);
                    }
                    lastPrintCount = totalMessages;
                }

                if (totalMessages >= 4_100_000) break;
            }

            // ── Final sync commit before exit ────────────────────────────
            // One sync commit at the end ensures offsets are durably saved.
            consumer.commitSync();

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

    private static Properties baseProps() {
        Properties p = new Properties();

        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,    "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,   "false");

        // ── Fetch tuning ─────────────────────────────────────────────────
        // Pull at least 1 MB per fetch instead of returning immediately
        // on the first available byte — fewer round-trips, higher throughput.
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");       // 1 MB

        // Give the broker up to 500 ms to accumulate FETCH_MIN_BYTES.
        // Keeps the consumer busy with large batches rather than tiny ones.
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");

        // Allow up to 50 MB per fetch response — the default 50 MB is fine
        // but stated explicitly so it's easy to tune.
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800");      // 50 MB

        // Process up to 5 000 records per poll call.
        // Larger batches amortise per-poll overhead across more messages.
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

        return p;
    }

    private static Thread startMemoryWatchdog() {
        Thread t = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long freeMb = readProcMemAvailable();
                    Runtime rt  = Runtime.getRuntime();
                    long jvmMb  = (rt.freeMemory()
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