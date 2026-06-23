package meta.fan.ms_kafka.init;

import meta.fan.ms_kafka.service.KafkaAdminService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class TopicInitializer {

    private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);
    private final KafkaAdminService kafkaAdminService;

    @Value("#{${topics:[]}}")
    private List<Map<String, Object>> topics;

    public TopicInitializer(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeTopics() {
        if (topics == null || topics.isEmpty()) {
            log.warn("No topics configured for initialization");
            return;
        }

        log.info("Initializing {} topics", topics.size());
        for (Map<String, Object> topicConfig : topics) {
            try {
                String name = (String) topicConfig.get("name");
                int partitions = (int) topicConfig.getOrDefault("partitions", 3);
                short replicationFactor = ((Number) topicConfig.getOrDefault("replication-factor", 1)).shortValue();

                if (name != null && !name.isEmpty()) {
                    kafkaAdminService.createTopic(name, partitions, replicationFactor);
                }
            } catch (Exception e) {
                log.error("Failed to initialize topic: {}", topicConfig, e);
            }
        }
    }
}