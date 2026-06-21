package meta.fan.ms_kafka.service;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final Tracer tracer;

    public KafkaProducerService(KafkaTemplate<String, byte[]> kafkaTemplate, Tracer tracer) {
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
    }

    public CompletableFuture<SendResult<String, byte[]>> publish(String topic, String key, byte[] payload) {
        Span span = tracer.spanBuilder("kafka-produce-" + topic).startSpan();
        try (var scope = span.makeCurrent()) {
            return kafkaTemplate.send(topic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) span.recordException(ex);
                    });
        } finally {
            span.end();
        }
    }
}