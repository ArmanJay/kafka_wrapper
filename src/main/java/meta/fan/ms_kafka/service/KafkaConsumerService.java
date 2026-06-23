package meta.fan.ms_kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(RedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "enterprise-topic", groupId = "default-wrapper-group")
    public void listen(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        try {
            log.info("Received message on topic: {} partition: {} offset: {}",
                    record.topic(), record.partition(), record.offset());

            // Process your business logic here
            String messageKey = record.key();
            byte[] messageValue = record.value();

            // Example: Cache processed messages with TTL
            if (messageKey != null && messageValue != null) {
                String cacheKey = "processed:" + messageKey;
                redisTemplate.opsForValue().set(cacheKey, messageValue, Duration.ofHours(24));
                log.debug("Cached message with key: {}", cacheKey);
            }

            // Store processing metadata
            String metadataKey = "metadata:" + record.topic() + ":" + record.partition() + ":" + record.offset();
            redisTemplate.opsForHash().put(metadataKey, "processed_at", System.currentTimeMillis());
            redisTemplate.opsForHash().put(metadataKey, "key", messageKey);
            redisTemplate.expire(metadataKey, Duration.ofDays(7));

            ack.acknowledge();
            log.info("Message processed and acknowledged successfully");
        } catch (Exception e) {
            log.error("Failed to process message. Routing to DLT.", e);
            throw e; // Throws to the DefaultErrorHandler to trigger DLT
        }
    }

    @Cacheable(value = "processedMessages", key = "#key")
    public byte[] getProcessedMessage(String key) {
        // This method is for demonstration - Redis cache will be used
        return null;
    }
}