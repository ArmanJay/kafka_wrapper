package meta.fan.ms_kafka.performance;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import meta.fan.ms_kafka.MsKafkaApplication;
import meta.fan.ms_kafka.grpc.producer.ProduceRequest;
import meta.fan.ms_kafka.grpc.producer.ProduceResponse;
import meta.fan.ms_kafka.grpc.producer.ProducerServiceGrpc;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = MsKafkaApplication.class)
@EmbeddedKafka(partitions = 5, topics = {"perf-test-topic"})
@DirtiesContext
public class KafkaPerformanceTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaPerformanceTest.class);
    private static final String TEST_TOPIC = "perf-test-topic";
    private static final int WARMUP_MESSAGES = 1000;
    private static final int TEST_MESSAGES = 10000;

    private static ManagedChannel channel;
    private static ProducerServiceGrpc.ProducerServiceBlockingStub stub;

    @BeforeAll
    static void setup() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();
        stub = ProducerServiceGrpc.newBlockingStub(channel);
    }

    @Test
    void testLatencyPercentiles() throws Exception {
        log.info("=== Starting Latency Percentile Test ===");

        // Warmup
        log.info("Warming up...");
        for (int i = 0; i < WARMUP_MESSAGES; i++) {
            sendMessage("warmup-" + i, "Warmup message".getBytes());
        }

        // Test
        List<Long> latencies = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(TEST_MESSAGES);
        ExecutorService executor = Executors.newFixedThreadPool(10);

        long startTime = System.nanoTime();

        for (int i = 0; i < TEST_MESSAGES; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    long messageStart = System.nanoTime();
                    String key = "perf-key-" + index;
                    byte[] payload = ("Performance test message " + index).getBytes();

                    sendMessage(key, payload);

                    long messageEnd = System.nanoTime();
                    latencies.add(messageEnd - messageStart);
                    latch.countDown();
                } catch (Exception e) {
                    log.error("Failed to send message {}", index, e);
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(120, TimeUnit.SECONDS), "Test timed out");
        long endTime = System.nanoTime();

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        // Calculate statistics
        long durationMs = (endTime - startTime) / 1_000_000;
        double throughput = (double) TEST_MESSAGES / (durationMs / 1000.0);

        // Sort latencies for percentile calculation
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        sortedLatencies.sort(Long::compareTo);

        double p50 = getPercentile(sortedLatencies, 50);
        double p75 = getPercentile(sortedLatencies, 75);
        double p95 = getPercentile(sortedLatencies, 95);
        double p99 = getPercentile(sortedLatencies, 99);
        double p999 = getPercentile(sortedLatencies, 99.9);

        log.info("=== Latency Percentile Test Results ===");
        log.info("Messages sent: {}", TEST_MESSAGES);
        log.info("Total time: {} ms", durationMs);
        log.info("Throughput: {:.2f} msg/sec", throughput);
        log.info("Latency Percentiles:");
        log.info("  P50: {:.2f} ms", p50 / 1_000_000.0);
        log.info("  P75: {:.2f} ms", p75 / 1_000_000.0);
        log.info("  P95: {:.2f} ms", p95 / 1_000_000.0);
        log.info("  P99: {:.2f} ms", p99 / 1_000_000.0);
        log.info("  P99.9: {:.2f} ms", p999 / 1_000_000.0);
        log.info("Min: {:.2f} ms", sortedLatencies.get(0) / 1_000_000.0);
        log.info("Max: {:.2f} ms", sortedLatencies.get(sortedLatencies.size() - 1) / 1_000_000.0);
        log.info("Avg: {:.2f} ms", latencies.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0);

        // Assertions
        assertTrue(throughput > 100, "Throughput too low: " + throughput + " msg/sec");
        assertTrue(p95 / 1_000_000.0 < 500, "P95 latency too high: " + p95 / 1_000_000.0 + "ms");
    }

    @Test
    void testThroughputWithDifferentPayloadSizes() throws Exception {
        log.info("=== Starting Throughput by Payload Size Test ===");

        int[] payloadSizes = {64, 256, 1024, 4096, 16384}; // 64B, 256B, 1KB, 4KB, 16KB
        int messagesPerSize = 2000;

        for (int size : payloadSizes) {
            log.info("Testing with {} byte payloads...", size);

            // Warmup
            for (int i = 0; i < 100; i++) {
                sendMessage("warmup-" + i, generatePayload(size, i));
            }

            // Test
            long startTime = System.nanoTime();
            CountDownLatch latch = new CountDownLatch(messagesPerSize);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicLong totalBytes = new AtomicLong(0);

            for (int i = 0; i < messagesPerSize; i++) {
                final int index = i;
                new Thread(() -> {
                    try {
                        String key = "size-test-" + size + "-" + index;
                        byte[] payload = generatePayload(size, index);
                        sendMessage(key, payload);
                        successCount.incrementAndGet();
                        totalBytes.addAndGet(payload.length);
                    } catch (Exception e) {
                        log.error("Failed to send message", e);
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            assertTrue(latch.await(60, TimeUnit.SECONDS), "Test timed out");
            long endTime = System.nanoTime();

            long durationMs = (endTime - startTime) / 1_000_000;
            double throughput = (double) successCount.get() / (durationMs / 1000.0);
            double mbps = (totalBytes.get() / (1024.0 * 1024.0)) / (durationMs / 1000.0);

            log.info("  Payload size: {} bytes", size);
            log.info("  Messages/sec: {:.2f}", throughput);
            log.info("  MB/sec: {:.2f}", mbps);
            log.info("  Success rate: {:.2f}%", (double) successCount.get() / messagesPerSize * 100);

            assertTrue(throughput > 50, "Throughput too low for " + size + " byte messages");
        }
    }

    @Test
    void testBurstHandling() throws Exception {
        log.info("=== Starting Burst Handling Test ===");

        int burstSize = 5000;
        int burstCount = 5;
        int delayBetweenBursts = 1000; // 1 second

        for (int burst = 0; burst < burstCount; burst++) {
            log.info("Burst {}: Sending {} messages...", burst + 1, burstSize);

            long burstStart = System.nanoTime();
            CountDownLatch latch = new CountDownLatch(burstSize);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < burstSize; i++) {
                final int index = i;
                int finalBurst = burst;
                new Thread(() -> {
                    try {
                        String key = "burst-" + finalBurst + "-" + index;
                        byte[] payload = ("Burst message " + finalBurst + "-" + index).getBytes();
                        sendMessage(key, payload);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        log.error("Failed to send message", e);
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS), "Burst " + burst + " timed out");
            long burstEnd = System.nanoTime();

            long burstDurationMs = (burstEnd - burstStart) / 1_000_000;
            double burstThroughput = (double) successCount.get() / (burstDurationMs / 1000.0);

            log.info("  Burst {} completed in {} ms", burst + 1, burstDurationMs);
            log.info("  Throughput: {:.2f} msg/sec", burstThroughput);
            log.info("  Success rate: {:.2f}%", (double) successCount.get() / burstSize * 100);

            assertTrue(successCount.get() > burstSize * 0.95,
                    "Too many failures in burst " + burst);

            if (burst < burstCount - 1) {
                Thread.sleep(delayBetweenBursts);
            }
        }
    }

    @Test
    void testResourceUtilization() throws Exception {
        log.info("=== Starting Resource Utilization Test ===");

        int durationSeconds = 30;
        int messagesPerSecond = 100;

        log.info("Running for {} seconds at {} msg/sec", durationSeconds, messagesPerSecond);

        AtomicLong totalMessages = new AtomicLong(0);
        AtomicLong totalBytes = new AtomicLong(0);
        AtomicInteger failures = new AtomicInteger(0);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        CountDownLatch latch = new CountDownLatch(durationSeconds);

        for (int second = 0; second < durationSeconds; second++) {
            scheduler.schedule(() -> {
                try {
                    for (int i = 0; i < messagesPerSecond; i++) {
                        try {
                            String key = "resource-test-" + System.currentTimeMillis() + "-" + i;
                            byte[] payload = ("Resource test message " + i).getBytes();
                            sendMessage(key, payload);
                            totalMessages.incrementAndGet();
                            totalBytes.addAndGet(payload.length);
                        } catch (Exception e) {
                            failures.incrementAndGet();
                            log.error("Failed to send message", e);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }, second, TimeUnit.SECONDS);
        }

        assertTrue(latch.await(durationSeconds + 10, TimeUnit.SECONDS), "Test timed out");
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);

        log.info("=== Resource Utilization Test Results ===");
        log.info("Total messages sent: {}", totalMessages.get());
        log.info("Total data sent: {:.2f} MB", totalBytes.get() / (1024.0 * 1024.0));
        log.info("Failures: {}", failures.get());
        log.info("Average msg/sec: {:.2f}", totalMessages.get() / (double) durationSeconds);
        log.info("Average MB/sec: {:.2f}",
                (totalBytes.get() / (1024.0 * 1024.0)) / (double) durationSeconds);

        assertTrue(failures.get() < totalMessages.get() * 0.01,
                "Too many failures: " + failures.get());
        assertTrue(totalMessages.get() > messagesPerSecond * durationSeconds * 0.8,
                "Throughput too low");
    }

    // Helper methods
    private void sendMessage(String key, byte[] payload) {
        ProduceRequest request = ProduceRequest.newBuilder()
                .setTopic(TEST_TOPIC)
                .setKey(key)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        ProduceResponse response = stub.sendMessage(request);
        if (!response.getSuccess()) {
            throw new RuntimeException("Failed to send message: " + response.getErrorMessage());
        }
    }

    private byte[] generatePayload(int size, int seed) {
        byte[] payload = new byte[size];
        new Random(seed).nextBytes(payload);
        return payload;
    }

    private double getPercentile(List<Long> sortedList, double percentile) {
        if (sortedList.isEmpty()) return 0;
        int index = (int) Math.ceil((percentile / 100.0) * sortedList.size()) - 1;
        return sortedList.get(Math.max(0, Math.min(index, sortedList.size() - 1)));
    }
}