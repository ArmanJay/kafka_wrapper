package meta.fan.ms_kafka.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import meta.fan.ms_kafka.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ValueOperations<String, Object> valueOperations;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @Mock
    private Acknowledgment acknowledgment;

    private KafkaConsumerService consumerService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // Only mock what's needed for all tests
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        consumerService = new KafkaConsumerService(redisTemplate, new ObjectMapper());
    }

    @Test
    void testListen_success() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, "test-key", "test-value".getBytes()
        );

        consumerService.listen(record, acknowledgment);

        verify(valueOperations).set(
                eq("processed:test-key"),
                any(byte[].class),
                any(Duration.class)
        );
        verify(hashOperations).put(anyString(), anyString(), anyLong());
        verify(redisTemplate).expire(anyString(), any(Duration.class));
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testListen_withNullValue() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, "test-key", null
        );

        consumerService.listen(record, acknowledgment);

        verify(valueOperations, never()).set(anyString(), any(), any(Duration.class));
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testListen_withNullKey() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, null, "test-value".getBytes()
        );

        consumerService.listen(record, acknowledgment);

        verify(valueOperations, never()).set(anyString(), any(), any(Duration.class));
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testListen_withException_shouldThrow() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "enterprise-topic", 0, 0L, "test-key", "test".getBytes()
        );

        // Mock to throw exception when setting cache
        doThrow(new RuntimeException("Redis error"))
                .when(valueOperations).set(anyString(), any(), any(Duration.class));

        // Should throw exception and not acknowledge
        assertThrows(RuntimeException.class, () -> {
            consumerService.listen(record, acknowledgment);
        });

        verify(acknowledgment, never()).acknowledge();
    }
}