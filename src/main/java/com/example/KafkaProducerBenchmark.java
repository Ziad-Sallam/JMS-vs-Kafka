package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerBenchmark {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "benchmark-topic";
    private static final int RESPONSE_RUNS = 1000;

    private static final long MEMORY_FLOOR_MB = 500;
    private static final long JVM_HEAP_FLOOR_MB = 50;

    private static final int THROUGHPUT_START = 100;
    private static final int THROUGHPUT_CEILING = 10_500_000;

    private static final AtomicBoolean ABORT = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {

        String payload = loadPayload("src/message.txt");
        System.out.printf("Payload size: %d bytes%n", payload.getBytes().length);

        Thread watchdog = startMemoryWatchdog();

        try {
            benchmarkResponseTime(payload);
            if (!ABORT.get()) benchmarkThroughput(payload);
        } finally {
            watchdog.interrupt();
        }
    }

    // ── Payload loader ─────────────────────────────────────────────
    private static String loadPayload(String path) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    // ── Response Time ──────────────────────────────────────────────
    private static void benchmarkResponseTime(String payload) throws Exception {
        System.out.println("\n=== Response Time ===");

        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            for (int i = 0; i < 50; i++) {
                producer.send(new ProducerRecord<>(TOPIC, payload)).get();
            }

            List<Long> latencies = new ArrayList<>();
            

            for (int i = 0; i < RESPONSE_RUNS && !ABORT.get(); i++) {
                long start = System.nanoTime();
                producer.send(new ProducerRecord<>(TOPIC, payload)).get();
                long elapsed = (System.nanoTime() - start) / 1_000_000;
                latencies.add(elapsed);
            }

            printStats(latencies);
        }
    }

    // ── Throughput ─────────────────────────────────────────────────
    private static void benchmarkThroughput(String payload) throws Exception {
        System.out.println("\n=== Throughput ===");

        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            int lastSuccess = 0;
            int rate = THROUGHPUT_START;

            while (rate <= THROUGHPUT_CEILING && !ABORT.get()) {

                boolean ok = runThroughputTest(producer, rate, payload);

                System.out.printf("Rate %d msg/s → %s%n",
                        rate, ok ? "OK" : "FAILED");

                if (!ok) {
                    System.out.printf("Max throughput: %d msg/s%n", lastSuccess);
                    return;
                }

                lastSuccess = rate;
                rate *= 2;
          
            }
        }
    }

    private static boolean runThroughputTest(KafkaProducer<String, String> producer,
                                             int targetRate,
                                             String payload) throws Exception {

        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(targetRate);

        long start = System.nanoTime();

        for (int i = 0; i < targetRate; i++) {

            if (ABORT.get()) break;

            producer.send(new ProducerRecord<>(TOPIC, payload), (m, e) -> {
                if (e != null) errors.incrementAndGet();
                latch.countDown();
            });

            // simple pacing (millisecond-level, realistic)
        }

        boolean allDone = latch.await(30, TimeUnit.SECONDS);

        producer.flush();

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        int failed = errors.get() + (allDone ? 0 : (int) latch.getCount());

        double actualRate = (targetRate - failed) / (durationMs / 1000.0);

        System.out.printf("   duration: %d ms | failed: %d | actual: %.2f msg/s%n",
                durationMs, failed, actualRate);

        return failed == 0 && allDone && !ABORT.get();
    }

    // ── Watchdog ──────────────────────────────────────────────────
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
                        System.out.println("[WATCHDOG] Memory low → aborting");
                        return;
                    }


                } catch (Exception ignored) {}
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static long readProcMemAvailable() {
        try (BufferedReader br = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("MemAvailable:")) {
                    return Long.parseLong(line.split("\\s+")[1]) / 1024;
                }
            }
        } catch (Exception ignored) {}
        return -1;
    }

    private static Properties baseProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return p;
    }

    private static void printStats(List<Long> values) {
        Collections.sort(values);
        int n = values.size();
        System.out.printf("Median: %d ms | Min: %d | Max: %d | P95: %d%n",
                values.get(n / 2),
                values.get(0),
                values.get(n - 1),
                values.get((int)(n * 0.95)));
    }
}