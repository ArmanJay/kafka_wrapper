package meta.fan.ms_kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class KafkaConnectService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectService.class);
    private final RestTemplate restTemplate;

    // URL to our Docker container running Kafka Connect
    private final String CONNECT_URL = "http://localhost:8083/connectors";

    public KafkaConnectService() {
        this.restTemplate = new RestTemplate();
    }

    /**
     * Dynamically deploy a new Connector (e.g., PostgreSQL Source or S3 Sink)
     */
    public void createConnector(String connectorName, Map<String, String> connectorConfig) {
        try {
            Map<String, Object> payload = Map.of(
                    "name", connectorName,
                    "config", connectorConfig
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(CONNECT_URL, request, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Successfully deployed connector: {}", connectorName);
            }
        } catch (Exception e) {
            log.error("Failed to deploy connector: {}", connectorName, e);
            throw new RuntimeException("Kafka Connect API error", e);
        }
    }

    /**
     * Check the status of a running connector
     */
    public String getConnectorStatus(String connectorName) {
        try {
            return restTemplate.getForObject(CONNECT_URL + "/" + connectorName + "/status", String.class);
        } catch (Exception e) {
            log.error("Failed to fetch status for connector: {}", connectorName, e);
            return "ERROR";
        }
    }
}