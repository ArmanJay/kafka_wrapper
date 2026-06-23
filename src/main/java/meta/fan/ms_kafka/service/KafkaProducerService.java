package meta.fan.ms_kafka.service;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObservationRegistry observationRegistry;

    public KafkaProducerService(KafkaTemplate<String, byte[]> kafkaTemplate,
                                ObservationRegistry observationRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.observationRegistry = observationRegistry;
    }

    public CompletableFuture<SendResult<String, byte[]>> publish(String topic, String key, byte[] payload) {
        Observation observation = Observation.createNotStarted("kafka-produce-" + topic, observationRegistry)
                .lowCardinalityKeyValue("kafka.topic", topic)
                .highCardinalityKeyValue("kafka.key", key != null ? key : "null")
                .highCardinalityKeyValue("payload.size", String.valueOf(payload.length));

        return observation.observe(() -> {
            log.info("Publishing message to topic: {}, key: {}, size: {} bytes", topic, key, payload.length);

            return kafkaTemplate.send(topic, key, payload)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish message to topic: {}", topic, ex);
                            if (ex instanceof TimeoutException) {
                                observation.highCardinalityKeyValue("error.type", "timeout");
                            }
                        } else {
                            log.info("Successfully published message to topic: {}, partition: {}, offset: {}",
                                    topic, result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                            observation.highCardinalityKeyValue("kafka.partition",
                                    String.valueOf(result.getRecordMetadata().partition()));
                            observation.highCardinalityKeyValue("kafka.offset",
                                    String.valueOf(result.getRecordMetadata().offset()));
                        }
                    });
        });
    }
}