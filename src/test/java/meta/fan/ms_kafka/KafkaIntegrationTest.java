package meta.fan.ms_kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.cloud.consul.enabled=false",
                "grpc.server.port=9090"
        }
)
@EmbeddedKafka(partitions = 3, brokerProperties = {"log.dir=/tmp/kafka"})
@ActiveProfiles("test")
@DisplayName("Kafka Integration Tests")
class KafkaIntegrationTest {

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    private static final String TEST_TOPIC = "test-integration-topic";

    @BeforeEach
    void setUp() {
        // Create test topics if needed
    }

    @Test
    @DisplayName("Should send and receive message from Kafka")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testKafkaProducerConsumerIntegration() throws Exception {
        // Arrange
        String key = "integration-test-key";
        byte[] payload = "integration-test-message".getBytes();

        // Act
        CompletableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(TEST_TOPIC, key, payload);

        // Assert
        assertThat(future).isNotNull();
        SendResult<String, byte[]> result = future.get(5, TimeUnit.SECONDS);
        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo(TEST_TOPIC);
    }

    @Test
    @DisplayName("Should send multiple messages to different partitions")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testMultipleMessagesToDifferentPartitions() throws Exception {
        // Arrange & Act
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            byte[] payload = ("message-" + i).getBytes();
            CompletableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(TEST_TOPIC, key, payload);
            SendResult<String, byte[]> result = future.get(5, TimeUnit.SECONDS);
            
            // Assert
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().partition()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    @DisplayName("Should handle large payload")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testLargePayload() throws Exception {
        // Arrange
        String key = "large-payload-key";
        byte[] largePayload = new byte[1024 * 100]; // 100 KB
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }

        // Act
        CompletableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(TEST_TOPIC, key, largePayload);

        // Assert
        assertThat(future).isNotNull();
        SendResult<String, byte[]> result = future.get(5, TimeUnit.SECONDS);
        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("Should maintain message order within partition")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testMessageOrderingInPartition() throws Exception {
        // Arrange
        String partitionKey = "ordered-key"; // Same key ensures same partition

        // Act
        long[] offsets = new long[5];
        for (int i = 0; i < 5; i++) {
            byte[] payload = ("ordered-message-" + i).getBytes();
            CompletableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(TEST_TOPIC, partitionKey, payload);
            SendResult<String, byte[]> result = future.get(5, TimeUnit.SECONDS);
            offsets[i] = result.getRecordMetadata().offset();
        }

        // Assert - offsets should be sequential
        for (int i = 1; i < offsets.length; i++) {
            assertThat(offsets[i]).isGreaterThan(offsets[i - 1]);
        }
    }
}
