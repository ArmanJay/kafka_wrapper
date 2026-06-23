package meta.fan.ms_kafka.unit;

import meta.fan.ms_kafka.service.KafkaProducerService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceSimpleTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    private KafkaProducerService producerService;

    @BeforeEach
    void setUp() {
        // Use a real ObservationRegistry or null if not critical
        producerService = new KafkaProducerService(kafkaTemplate, null);
    }

    @Test
    void testPublish_success() throws Exception {
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = "test".getBytes();

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(topic, 0), 0L, 0, 0L, 0L, 0, 0
        );
        SendResult<String, byte[]> sendResult = new SendResult<>(
                new ProducerRecord<>(topic, key, payload), metadata
        );

        CompletableFuture<SendResult<String, byte[]>> future =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        CompletableFuture<SendResult<String, byte[]>> result =
                producerService.publish(topic, key, payload);

        assertNotNull(result);
        SendResult<String, byte[]> actualResult = result.get();
        assertNotNull(actualResult);
        assertEquals(metadata, actualResult.getRecordMetadata());

        verify(kafkaTemplate).send(topic, key, payload);
    }

    @Test
    void testPublish_withException() {
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = "test".getBytes();

        CompletableFuture<SendResult<String, byte[]>> future =
                CompletableFuture.failedFuture(new RuntimeException("Kafka error"));

        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        CompletableFuture<SendResult<String, byte[]>> result =
                producerService.publish(topic, key, payload);

        assertNotNull(result);
        assertTrue(result.isCompletedExceptionally());

        verify(kafkaTemplate).send(topic, key, payload);
    }
}