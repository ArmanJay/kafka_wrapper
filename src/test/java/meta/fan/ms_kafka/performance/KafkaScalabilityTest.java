package meta.fan.ms_kafka.performance;

import meta.fan.ms_kafka.MsKafkaApplication;
import meta.fan.ms_kafka.service.KafkaProducerService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = MsKafkaApplication.class)
@EmbeddedKafka(partitions = 10,
        topics = {"scalability-test-topic", "performance-test-topic"},
        brokerProperties = {
                "num.partitions=10",
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        })
@DirtiesContext
public class KafkaScalabilityTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaScalabilityTest.class);
    private static final String TEST_TOPIC = "scalability-test-topic";
    private static final int MESSAGE_SIZE = 1024; // 1KB messages

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaProducerService producerService;

    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @BeforeEach
    void setUp() {
        // Create a producer for direct Kafka operations
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760); // 10MB
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        ProducerFactory<String, byte[]> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    void testHighThroughput() throws Exception {
        log.info("=== Starting High Throughput Test ===");
        int messageCount = 100000;
        int batchSize = 1000;
        int concurrency = 10;

        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong totalBytes = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i += batchSize) {
            final int start = i;
            final int end = Math.min(i + batchSize, messageCount);

            executor.submit(() -> {
                for (int j = start; j < end; j++) {
                    try {
                        String key = "key-" + j;
                        byte[] payload = generatePayload(MESSAGE_SIZE, j);

                        long messageStart = System.nanoTime();
                        var future = producerService.publish(TEST_TOPIC, key, payload);
                        var result = future.get(5, TimeUnit.SECONDS);
                        long messageEnd = System.nanoTime();

                        successCount.incrementAndGet();
                        totalTime.addAndGet(messageEnd - messageStart);
                        totalBytes.addAndGet(payload.length);
                        latch.countDown();

                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                        latch.countDown();
                        log.error("Failed to send message {}", j, e);
                    }
                }
            });
        }

        boolean completed = latch.await(120, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long durationMs = endTime - startTime;
        double throughput = (double) successCount.get() / (durationMs / 1000.0);

        log.info("=== High Throughput Test Results ===");
        log.info("Total messages attempted: {}", messageCount);
        log.info("Successfully sent: {}", successCount.get());
        log.info("Failed: {}", failureCount.get());
        log.info("Duration: {} ms", durationMs);
        log.info("Throughput: {:.2f} messages/second", throughput);
        log.info("Average latency: {:.2f} ms",
                successCount.get() > 0 ? (totalTime.get() / successCount.get() / 1_000_000.0) : 0);
        log.info("Total data sent: {:.2f} MB", totalBytes.get() / (1024.0 * 1024.0));
        log.info("Network throughput: {:.2f} MB/s",
                (totalBytes.get() / (1024.0 * 1024.0)) / (durationMs / 1000.0));

        assertTrue(completed, "Test timed out");
        assertTrue(successCount.get() > 0, "No messages were sent successfully");
        assertTrue(throughput > 100, "Throughput too low: " + throughput + " msg/sec");
    }

    @Test
    void testConcurrentConsumers() throws Exception {
        log.info("=== Starting Concurrent Consumer Test ===");
        int producerMessages = 10000;
        int consumerCount = 5;
        String consumerGroup = "scalability-consumer-group";

        // First, produce messages
        log.info("Producing {} messages...", producerMessages);
        CountDownLatch produceLatch = new CountDownLatch(producerMessages);
        for (int i = 0; i < producerMessages; i++) {
            String key = "prod-key-" + i;
            byte[] payload = generatePayload(512, i);
            kafkaTemplate.send(TEST_TOPIC, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            produceLatch.countDown();
                        }
                    });
        }
        assertTrue(produceLatch.await(60, TimeUnit.SECONDS), "Failed to produce all messages");

        // Create multiple consumers
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(consumerCount);
        AtomicInteger totalConsumed = new AtomicInteger(0);
        CountDownLatch consumeLatch = new CountDownLatch(producerMessages);

        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            consumerExecutor.submit(() -> {
                try {
                    Map<String, Object> consumerProps = new HashMap<>(
                            KafkaTestUtils.consumerProps(consumerGroup + "-" + consumerId, "true", embeddedKafkaBroker)
                    );
                    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            StringDeserializer.class.getName());
                    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            ByteArrayDeserializer.class.getName());
                    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
                    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

                    try (Consumer<String, byte[]> consumer =
                                 new DefaultKafkaConsumerFactory<String, byte[]>(consumerProps).createConsumer()) {
                        consumer.subscribe(Collections.singletonList(TEST_TOPIC));

                        int consumedInThisConsumer = 0;
                        while (consumedInThisConsumer < producerMessages / consumerCount) {
                            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                            if (!records.isEmpty()) {
                                int count = records.count();
                                consumedInThisConsumer += count;
                                totalConsumed.addAndGet(count);
                                for (int j = 0; j < count; j++) {
                                    consumeLatch.countDown();
                                }
                                consumer.commitSync();
                                log.debug("Consumer {} consumed {} messages (total: {})",
                                        consumerId, count, consumedInThisConsumer);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("Consumer {} failed", consumerId, e);
                }
            });
        }

        boolean consumed = consumeLatch.await(120, TimeUnit.SECONDS);
        consumerExecutor.shutdown();
        consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);

        log.info("=== Concurrent Consumer Test Results ===");
        log.info("Messages produced: {}", producerMessages);
        log.info("Messages consumed: {}", totalConsumed.get());
        log.info("Consumers used: {}", consumerCount);
        log.info("All messages consumed: {}", consumed);

        assertEquals(producerMessages, totalConsumed.get(), "Not all messages were consumed");
        assertTrue(consumed, "Consumption timed out");
    }

    @Test
    void testLargeMessageHandling() throws Exception {
        log.info("=== Starting Large Message Handling Test ===");
        int[] messageSizes = {1024, 10240, 102400, 1048576, 5242880}; // 1KB, 10KB, 100KB, 1MB, 5MB
        int messagesPerSize = 10;

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int size : messageSizes) {
            log.info("Testing {} KB messages...", size / 1024);
            for (int i = 0; i < messagesPerSize; i++) {
                try {
                    String key = "large-key-" + size + "-" + i;
                    byte[] payload = generatePayload(size, i);

                    long start = System.nanoTime();
                    var future = producerService.publish(TEST_TOPIC, key, payload);
                    var result = future.get(10, TimeUnit.SECONDS);
                    long end = System.nanoTime();

                    successCount.incrementAndGet();
                    log.debug("Sent {} bytes message in {} ms",
                            size, (end - start) / 1_000_000);

                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    log.error("Failed to send {} byte message", size, e);
                }
            }
        }

        log.info("=== Large Message Test Results ===");
        log.info("Total messages: {}", messageSizes.length * messagesPerSize);
        log.info("Successful: {}", successCount.get());
        log.info("Failed: {}", failureCount.get());

        assertTrue(failureCount.get() == 0, "Some large messages failed");
    }

    @Test
    void testPartitionRebalancing() throws Exception {
        log.info("=== Starting Partition Rebalancing Test ===");
        String topic = "rebalance-test-topic";
        int partitions = 20;
        int messagesPerPartition = 100;

        // Create topic with many partitions
        embeddedKafkaBroker.addTopics(topic);

        // Produce messages across all partitions
        log.info("Producing messages across {} partitions...", partitions);
        CountDownLatch produceLatch = new CountDownLatch(partitions * messagesPerPartition);

        for (int p = 0; p < partitions; p++) {
            for (int m = 0; m < messagesPerPartition; m++) {
                String key = "partition-" + p + "-msg-" + m;
                byte[] payload = generatePayload(256, m);
                kafkaTemplate.send(topic, key, payload)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                produceLatch.countDown();
                            }
                        });
            }
        }
        assertTrue(produceLatch.await(60, TimeUnit.SECONDS), "Failed to produce all messages");

        // Test rebalancing with multiple consumers joining/leaving
        int consumerCount = 5;
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(consumerCount);
        AtomicInteger totalConsumed = new AtomicInteger(0);
        CountDownLatch consumeLatch = new CountDownLatch(partitions * messagesPerPartition);

        List<Consumer<String, byte[]>> consumers = new CopyOnWriteArrayList<>();

        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            consumerExecutor.submit(() -> {
                try {
                    Map<String, Object> consumerProps = new HashMap<>(
                            KafkaTestUtils.consumerProps("rebalance-group-" + consumerId, "true", embeddedKafkaBroker)
                    );
                    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            StringDeserializer.class.getName());
                    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            ByteArrayDeserializer.class.getName());
                    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
                    consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                            "org.apache.kafka.clients.consumer.RoundRobinAssignor");

                    try (Consumer<String, byte[]> consumer =
                                 new DefaultKafkaConsumerFactory<String, byte[]>(consumerProps).createConsumer()) {
                        consumers.add(consumer);
                        consumer.subscribe(Collections.singletonList(topic));

                        while (totalConsumed.get() < partitions * messagesPerPartition) {
                            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                            if (!records.isEmpty()) {
                                int count = records.count();
                                totalConsumed.addAndGet(count);
                                for (int j = 0; j < count; j++) {
                                    consumeLatch.countDown();
                                }
                                consumer.commitSync();
                                log.debug("Consumer {} consumed {} messages (total: {})",
                                        consumerId, count, totalConsumed.get());
                            }

                            // Simulate consumer rebalancing by adding/removing consumers
                            if (consumerId == 0 && totalConsumed.get() > (partitions * messagesPerPartition) / 2) {
                                // One consumer leaves - triggers rebalance
                                if (!consumers.isEmpty()) {
                                    Consumer<String, byte[]> lastConsumer = consumers.remove(0);
                                    lastConsumer.close();
                                    log.info("Consumer {} left the group - triggering rebalance", consumerId);
                                    Thread.sleep(2000); // Wait for rebalance
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("Consumer {} failed", consumerId, e);
                }
            });
        }

        boolean consumed = consumeLatch.await(180, TimeUnit.SECONDS);
        consumerExecutor.shutdown();
        consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);

        log.info("=== Partition Rebalancing Test Results ===");
        log.info("Total messages: {}", partitions * messagesPerPartition);
        log.info("Messages consumed: {}", totalConsumed.get());
        log.info("Consumers used: {}", consumerCount);
        log.info("All messages consumed: {}", consumed);

        assertEquals(partitions * messagesPerPartition, totalConsumed.get(), "Not all messages were consumed");
    }

    // Helper methods
    private byte[] generatePayload(int size, int seed) {
        byte[] payload = new byte[size];
        Random random = new Random(seed);
        random.nextBytes(payload);
        return payload;
    }
}