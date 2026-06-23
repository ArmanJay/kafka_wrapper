package meta.fan.ms_kafka;

import io.opentelemetry.api.trace.Tracer;
import meta.fan.ms_kafka.service.KafkaProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, SpringExtension.class})
@DisplayName("Kafka Producer Service Tests")
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Mock
    private Tracer tracer;

    @Mock
    private SendResult<String, byte[]> sendResult;

    private KafkaProducerService producerService;

    @BeforeEach
    void setUp() {
        producerService = new KafkaProducerService(kafkaTemplate, tracer);
    }

    @Test
    @DisplayName("Should successfully publish message to Kafka")
    void testPublishMessageSuccess() {
        // Arrange
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = "test-message".getBytes();

        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        future.complete(sendResult);

        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        // Act
        CompletableFuture<SendResult<String, byte[]>> result = producerService.publish(topic, key, payload);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.isDone()).isTrue();
        verify(kafkaTemplate, times(1)).send(topic, key, payload);
    }

    @Test
    @DisplayName("Should handle publish failure gracefully")
    void testPublishMessageFailure() {
        // Arrange
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = "test-message".getBytes();
        Exception exception = new RuntimeException("Kafka connection failed");

        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        future.completeExceptionally(exception);

        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        // Act
        CompletableFuture<SendResult<String, byte[]>> result = producerService.publish(topic, key, payload);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.isDone()).isTrue();
        assertThatThrownBy(result::get).hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("Should publish message with null payload")
    void testPublishMessageWithNullPayload() {
        // Arrange
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = null;

        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        future.complete(sendResult);

        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        // Act
        CompletableFuture<SendResult<String, byte[]>> result = producerService.publish(topic, key, payload);

        // Assert
        assertThat(result).isNotNull();
        verify(kafkaTemplate, times(1)).send(topic, key, payload);
    }

    @Test
    @DisplayName("Should publish multiple messages sequentially")
    void testPublishMultipleMessages() {
        // Arrange
        String topic = "test-topic";
        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        future.complete(sendResult);

        when(kafkaTemplate.send(anyString(), anyString(), any(byte[].class))).thenReturn(future);

        // Act
        for (int i = 0; i < 5; i++) {
            String key = "key-" + i;
            byte[] payload = ("message-" + i).getBytes();
            CompletableFuture<SendResult<String, byte[]>> result = producerService.publish(topic, key, payload);
            assertThat(result.isDone()).isTrue();
        }

        // Assert
        verify(kafkaTemplate, times(5)).send(anyString(), anyString(), any(byte[].class));
    }
}
