package meta.fan.ms_kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = "enterprise-topic", groupId = "default-wrapper-group")
    public void listen(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        try {
            log.info("Received message on topic: {} partition: {} offset: {}",
                    record.topic(), record.partition(), record.offset());

            // Process your business logic here

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process message. Routing to DLT.", e);
            throw e; // Throws to the DefaultErrorHandler to trigger DLT
        }
    }
}