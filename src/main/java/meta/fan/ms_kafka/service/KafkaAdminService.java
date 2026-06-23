package meta.fan.ms_kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    private final AdminClient adminClient;

    public KafkaAdminService(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public void createTopic(String topicName, int partitions, short replicationFactor) {
        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get();
            log.info("Successfully created topic: {} with {} partitions and replication factor {}",
                    topicName, partitions, replicationFactor);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.warn("Topic {} already exists", topicName);
            } else {
                log.error("Failed to create topic: {}", topicName, e);
                throw new RuntimeException("Failed to create topic: " + topicName, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while creating topic: {}", topicName, e);
            throw new RuntimeException("Interrupted while creating topic: " + topicName, e);
        }
    }

    public boolean topicExists(String topicName) {
        try {
            return adminClient.listTopics().names().get().contains(topicName);
        } catch (Exception e) {
            log.error("Failed to check if topic exists: {}", topicName, e);
            return false;
        }
    }
}