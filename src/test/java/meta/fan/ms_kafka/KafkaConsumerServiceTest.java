package meta.fan.ms_kafka;

import meta.fan.ms_kafka.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, SpringExtension.class})
@DisplayName("Kafka Consumer Service Tests")
class KafkaConsumerServiceTest {

    @Mock
    private Acknowledgment acknowledgment;

    private KafkaConsumerService consumerService;

    @BeforeEach
    void setUp() {
        consumerService = new KafkaConsumerService();
    }

    @Test
    @DisplayName("Should successfully consume message and acknowledge")
    void testListenMessageSuccess() {
        // Arrange
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, "test-key", "test-message".getBytes()
        );

        // Act
        consumerService.listen(consumerRecord, acknowledgment);

        // Assert
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Should handle consumer record with null value")
    void testListenMessageWithNullValue() {
        // Arrange
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, "test-key", null
        );

        // Act
        consumerService.listen(consumerRecord, acknowledgment);

        // Assert
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Should throw exception when acknowledgment fails")
    void testListenMessageWithAcknowledgmentFailure() {
        // Arrange
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, "test-key", "test-message".getBytes()
        );

        doThrow(new RuntimeException("Acknowledgment failed")).when(acknowledgment).acknowledge();

        // Act & Assert
        assertThatThrownBy(() -> {
            try {
                consumerService.listen(consumerRecord, acknowledgment);
            } catch (Exception e) {
                throw e;
            }
        }).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("Should process message from different partitions")
    void testListenMessageFromDifferentPartitions() {
        // Arrange
        for (int partition = 0; partition < 3; partition++) {
            ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                    "enterprise-topic", partition, (long) partition, "key-" + partition,
                    ("message-partition-" + partition).getBytes()
            );

            // Act
            consumerService.listen(consumerRecord, acknowledgment);
        }

        // Assert
        verify(acknowledgment, times(3)).acknowledge();
    }
}
