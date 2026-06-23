package meta.fan.ms_kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
    private final ObjectMapper objectMapper;

    @Value("${kafka.connect.url:http://localhost:8083/connectors}")
    private String connectUrl;

    public KafkaConnectService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Dynamically deploy a new Connector (e.g., PostgreSQL Source or S3 Sink)
     */
    public void createConnector(String connectorName, Map<String, String> connectorConfig) {
        try {
            // Check if connector already exists
            if (connectorExists(connectorName)) {
                log.warn("Connector {} already exists. Skipping creation.", connectorName);
                return;
            }

            Map<String, Object> payload = Map.of(
                    "name", connectorName,
                    "config", connectorConfig
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(connectUrl, request, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Successfully deployed connector: {}", connectorName);
            } else {
                log.error("Failed to deploy connector: {}. Status: {}", connectorName, response.getStatusCode());
                throw new RuntimeException("Failed to deploy connector: " + connectorName);
            }
        } catch (Exception e) {
            log.error("Failed to deploy connector: {}", connectorName, e);
            throw new RuntimeException("Kafka Connect API error", e);
        }
    }

    /**
     * Check if a connector exists
     */
    public boolean connectorExists(String connectorName) {
        try {
            String url = connectUrl + "/" + connectorName;
            restTemplate.getForObject(url, String.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check the status of a running connector
     */
    public String getConnectorStatus(String connectorName) {
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(
                    connectUrl + "/" + connectorName + "/status",
                    String.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = objectMapper.readTree(response.getBody());
                return jsonNode.path("connector").path("state").asText("UNKNOWN");
            }
            return "ERROR";
        } catch (Exception e) {
            log.error("Failed to fetch status for connector: {}", connectorName, e);
            return "ERROR";
        }
    }

    /**
     * Delete a connector
     */
    public void deleteConnector(String connectorName) {
        try {
            restTemplate.delete(connectUrl + "/" + connectorName);
            log.info("Successfully deleted connector: {}", connectorName);
        } catch (Exception e) {
            log.error("Failed to delete connector: {}", connectorName, e);
            throw new RuntimeException("Failed to delete connector: " + connectorName, e);
        }
    }
}