package meta.fan.ms_kafka.integration;

import meta.fan.ms_kafka.MsKafkaApplication;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = MsKafkaApplication.class)
@EmbeddedKafka(partitions = 1,
        topics = {"test-topic", "enterprise-topic", "orders-inbound"})
@DirtiesContext
class KafkaIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaIntegrationTest.class);

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Test
    void testKafkaTemplateExists() {
        assertNotNull(kafkaTemplate);
        log.info("✅ KafkaTemplate is available");
    }

    @Test
    void testSendMessage_success() throws Exception {
        String topic = "test-topic";
        String key = "test-key";
        String message = "Test message";

        // Send the message
        var future = kafkaTemplate.send(topic, key, message.getBytes());
        var result = future.get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertNotNull(result.getRecordMetadata());
        assertEquals(topic, result.getRecordMetadata().topic());
        assertTrue(result.getRecordMetadata().partition() >= 0);
        assertTrue(result.getRecordMetadata().offset() >= 0);

        log.info("✅ Message sent successfully to partition: {}, offset: {}",
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }

    @Test
    void testSendMessage_withoutKey() throws Exception {
        String topic = "test-topic";
        String message = "Message without key";

        var future = kafkaTemplate.send(topic, message.getBytes());
        var result = future.get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertNotNull(result.getRecordMetadata());
        log.info("✅ Message without key sent successfully");
    }

    @Test
    void testSendLargeMessage() throws Exception {
        String topic = "test-topic";
        byte[] payload = new byte[1024 * 10]; // 10KB

        var future = kafkaTemplate.send(topic, "large-key", payload);
        var result = future.get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertNotNull(result.getRecordMetadata());
        log.info("✅ Large message (10KB) sent successfully");
    }
}