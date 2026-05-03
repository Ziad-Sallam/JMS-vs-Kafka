package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaProducerBenchmarkReal
 *
 * Measures:
 *   1. Response Time — synchronous sends with timestamps embedded in the
 *      message payload so the consumer can compute end-to-end latency.
 *      Reports: median, min, max, p95, p99 (ms).
 *
 *   2. Throughput    — async sends at exponentially increasing rates.
 *      Reports: actual msg/s, MB/s, failed count, and wall-clock duration.
 *
 * Message format:
 *   <send_epoch_ms>|<original_payload>
 *
 * The consumer can split on '|' to recover the send timestamp and compute
 * end-to-end latency = receive_epoch_ms - send_epoch_ms.
 */
public class KafkaProducerBenchmark {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC             = "benchmark-topic";
    private static final String PAYLOAD_PATH      = "src/message.txt";

    private static final int  RESPONSE_RUNS    = 1_000;
    private static final int  WARMUP_RUNS      = 50;

    private static final int  THROUGHPUT_START   = 100;
    private static final int  THROUGHPUT_CEILING = 10_000_000;

    private static final long MEMORY_FLOOR_MB   = 500;
    private static final long JVM_HEAP_FLOOR_MB = 50;

    private static final AtomicBoolean ABORT = new AtomicBoolean(false);

    // ── Entry point ────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        String rawPayload = loadPayload(PAYLOAD_PATH);
        System.out.printf("Raw payload size : %d bytes%n", rawPayload.getBytes().length);
        System.out.printf("Timestamp prefix : <epoch_ms>|<payload>%n%n");

        Thread watchdog = startMemoryWatchdog();
        try {
            benchmarkResponseTime(rawPayload);
            if (!ABORT.get()) benchmarkThroughput(rawPayload);
        } finally {
            watchdog.interrupt();
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  1. Response Time
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Sends RESPONSE_RUNS messages synchronously (acks=1, linger=0).
     *
     * Each message is prefixed with System.currentTimeMillis() so a consumer
     * can compute end-to-end latency independently.
     *
     * The producer-side round-trip (send + broker ack) is also measured and
     * reported as latency percentiles.
     */
    private static void benchmarkResponseTime(String rawPayload) throws Exception {
        printHeader("RESPONSE TIME  (synchronous sends, acks=1, linger=0ms)");

        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG,      "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // ── Warm-up (discarded) ──────────────────────────────────────
            System.out.printf("Warm-up: %d sends (discarded)...%n", WARMUP_RUNS);
            for (int i = 0; i < WARMUP_RUNS; i++) {
                producer.send(new ProducerRecord<>(TOPIC, stampedPayload(rawPayload))).get();
            }

            // ── Measurement ──────────────────────────────────────────────
            System.out.printf("Measuring: %d synchronous sends...%n%n", RESPONSE_RUNS);
            List<Long> latenciesMs = new ArrayList<>(RESPONSE_RUNS);

            for (int i = 0; i < RESPONSE_RUNS && !ABORT.get(); i++) {
                String msg   = stampedPayload(rawPayload);   // embed send timestamp
                long   t0    = System.nanoTime();
                producer.send(new ProducerRecord<>(TOPIC, msg)).get();
                long   rtMs  = (System.nanoTime() - t0) / 1_000_000L;
                latenciesMs.add(rtMs);
            }

            printLatencyStats("Producer round-trip (send → ack)", latenciesMs);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  2. Throughput
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Sends messages asynchronously at exponentially increasing rates.
     * Each message still carries the send timestamp so a consumer can measure
     * end-to-end latency for every individual record.
     */
    private static void benchmarkThroughput(String rawPayload) throws Exception {
        printHeader("THROUGHPUT  (async sends, acks=1, linger=5ms, batch=64KB)");

        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG,       "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG,  "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            int lastGood = 0;
            int rate     = THROUGHPUT_START;

            System.out.printf("%-18s %12s %10s %10s %8s %10s%n",
                    "Target", "Actual msg/s", "MB/s", "Duration", "Failed", "Status");
            printDivider();

            while (rate <= THROUGHPUT_CEILING && !ABORT.get()) {

                ThroughputResult r = runLoadTest(producer, rate, rawPayload);

                boolean ok = r.failed == 0 && r.allAcked && !ABORT.get();
                System.out.printf("%-18s %12.0f %10.2f %8d ms %8d %10s%n",
                        rate + " msg/s",
                        r.actualRate,
                        r.mbPerSec,
                        r.durationMs,
                        r.failed,
                        ok ? "OK" : "FAILED");

                if (!ok) {
                    System.out.printf("%nBreakpoint reached at %,d msg/s  (last stable: %,d msg/s)%n",
                            rate, lastGood);
                    return;
                }

                lastGood = rate;
                rate    *= 2;
            }

            System.out.printf("%nAll rates up to %,d msg/s completed successfully.%n", lastGood);
        }
    }

    /** Fires {@code rate} async sends and waits for all acks. */
    private static ThroughputResult runLoadTest(KafkaProducer<String, String> producer,
                                                int rate,
                                                String rawPayload) throws Exception {
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(rate);

        long startNs = System.nanoTime();

        for (int i = 0; i < rate && !ABORT.get(); i++) {
            String msg = stampedPayload(rawPayload);
            producer.send(new ProducerRecord<>(TOPIC, msg), (meta, ex) -> {
                if (ex != null) errors.incrementAndGet();
                latch.countDown();
            });
        }

        boolean allAcked = latch.await(30, TimeUnit.SECONDS);
        producer.flush();

        long  durationMs = (System.nanoTime() - startNs) / 1_000_000L;
        int   failed     = errors.get() + (allAcked ? 0 : (int) latch.getCount());
        int   sent       = rate - failed;
        double msgRate   = sent / (durationMs / 1000.0);
        double mbPerSec  = (sent * (double) rawPayload.getBytes().length)
                           / (durationMs / 1000.0) / (1024 * 1024);

        return new ThroughputResult(msgRate, mbPerSec, durationMs, failed, allAcked);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Prepends the current epoch millisecond to the payload.
     * Format: {@code <epoch_ms>|<original_payload>}
     */
    private static String stampedPayload(String rawPayload) {
        return System.currentTimeMillis() + "|" + rawPayload;
    }

    private static String loadPayload(String path) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line).append('\n');
        }
        return sb.toString();
    }

    private static Properties baseProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    BOOTSTRAP_SERVERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class.getName());
        return p;
    }

    // ── Stats ──────────────────────────────────────────────────────────────

    private static void printLatencyStats(String label, List<Long> raw) {
        List<Long> sorted = new ArrayList<>(raw);
        Collections.sort(sorted);
        int n = sorted.size();

        long median = sorted.get(n / 2);
        long min    = sorted.get(0);
        long max    = sorted.get(n - 1);
        long p95    = sorted.get((int) Math.ceil(n * 0.95) - 1);
        long p99    = sorted.get((int) Math.ceil(n * 0.99) - 1);

        // Compute mean
        double mean = raw.stream().mapToLong(Long::longValue).average().orElse(0);

        // Compute std-dev
        double variance = raw.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average().orElse(0);
        double stdDev = Math.sqrt(variance);

        System.out.println("  " + label);
        System.out.println("  ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐");
        System.out.println("  │  Median  │   Mean   │  StdDev  │   Min    │   Max    │   p95    │   p99    │");
        System.out.println("  ├──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤");
        System.out.printf( "  │ %6d ms │ %6.1f ms │ %6.1f ms │ %6d ms │ %6d ms │ %6d ms │ %6d ms │%n",
                median, mean, stdDev, min, max, p95, p99);
        System.out.println("  └──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘");
        System.out.println();
    }

    private static void printHeader(String title) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.printf( "║  %-60s║%n", title);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    private static void printDivider() {
        System.out.println("──────────────────────────────────────────────────────────────");
    }

    // ── Watchdog ───────────────────────────────────────────────────────────

    private static Thread startMemoryWatchdog() {
        Thread t = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long freeMb = readMem();
                    Runtime rt  = Runtime.getRuntime();
                    long jvmMb  = (rt.freeMemory() + rt.maxMemory() - rt.totalMemory())
                                  / (1024 * 1024);
                    if ((freeMb != -1 && freeMb < MEMORY_FLOOR_MB)
                            || jvmMb < JVM_HEAP_FLOOR_MB) {
                        ABORT.set(true);
                        System.out.println("[WATCHDOG] Memory low → aborting");
                        return;
                    }
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception ignored) {}
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static long readMem() {
        try (BufferedReader br = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("MemAvailable:"))
                    return Long.parseLong(line.split("\\s+")[1]) / 1024;
            }
        } catch (Exception ignored) {}
        return -1;
    }

    // ── Value object ──────────────────────────────────────────────────────

    static class ThroughputResult {
        final double  actualRate, mbPerSec;
        final long    durationMs;
        final int     failed;
        final boolean allAcked;

        ThroughputResult(double r, double mb, long d, int f, boolean a) {
            actualRate = r; mbPerSec = mb; durationMs = d; failed = f; allAcked = a;
        }
    }
}