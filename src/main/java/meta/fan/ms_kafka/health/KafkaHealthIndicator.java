package meta.fan.ms_kafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final AdminClient adminClient;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Override
    public Health health() {
        try {
            // Try to list topics with a timeout
            var options = new ListTopicsOptions().timeoutMs(5000);
            var topics = adminClient.listTopics(options);
            var names = topics.names().get();

            return Health.up()
                    .withDetail("topics_count", names.size())
                    .withDetail("status", "Connected")
                    .build();
        } catch (Exception e) {
            return Health.down()
                    .withDetail("error", e.getMessage())
                    .withDetail("status", "Disconnected")
                    .build();
        }
    }
}