package meta.fan.ms_kafka.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import meta.fan.ms_kafka.grpc.producer.ProduceRequest;
import meta.fan.ms_kafka.grpc.producer.ProduceResponse;
import meta.fan.ms_kafka.grpc.producer.ProducerServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Standalone gRPC test client - Run this as a Java application
 * to test the gRPC producer endpoint
 */
public class GrpcTestClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcTestClient.class);

    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        try {
            ProducerServiceGrpc.ProducerServiceBlockingStub stub =
                    ProducerServiceGrpc.newBlockingStub(channel);

            // Test 1: Simple text message
            log.info("=== Test 1: Sending simple text message ===");
            sendTestMessage(stub, "test-topic", "key1", "Hello Kafka from gRPC!".getBytes());

            // Test 2: JSON message for Streams processing
            log.info("\n=== Test 2: Sending VIP JSON message for Streams ===");
            String jsonMessage = "{\"id\":123, \"type\":\"VIP\", \"customer\":\"John Doe\", \"total\":299.99}";
            sendTestMessage(stub, "orders-inbound", "vip-key-123", jsonMessage.getBytes());

            // Test 3: Non-VIP message (should be filtered by Streams)
            log.info("\n=== Test 3: Sending non-VIP message (should be filtered) ===");
            String nonVipMessage = "{\"id\":456, \"type\":\"REGULAR\", \"customer\":\"Jane Smith\", \"total\":50.00}";
            sendTestMessage(stub, "orders-inbound", "regular-key-456", nonVipMessage.getBytes());

            // Test 4: Enterprise topic
            log.info("\n=== Test 4: Sending to enterprise topic ===");
            sendTestMessage(stub, "enterprise-topic", "enterprise-key", "Enterprise message".getBytes());

            // Test 5: Large payload
            log.info("\n=== Test 5: Sending large payload ===");
            byte[] largePayload = new byte[1024 * 10]; // 10KB
            for (int i = 0; i < largePayload.length; i++) {
                largePayload[i] = (byte) (i % 256);
            }
            sendTestMessage(stub, "test-topic", "large-key", largePayload);

        } finally {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void sendTestMessage(
            ProducerServiceGrpc.ProducerServiceBlockingStub stub,
            String topic,
            String key,
            byte[] payload) {

        try {
            ProduceRequest request = ProduceRequest.newBuilder()
                    .setTopic(topic)
                    .setKey(key != null ? key : "")
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .build();

            ProduceResponse response = stub.sendMessage(request);

            if (response.getSuccess()) {
                log.info("✅ SUCCESS - Message sent to topic: {}", topic);
                log.info("   Partition: {}, Offset: {}", response.getPartition(), response.getOffset());
            } else {
                log.error("❌ FAILED - Error: {}", response.getErrorMessage());
            }

        } catch (Exception e) {
            log.error("❌ EXCEPTION - Failed to send message: {}", e.getMessage());
        }
    }
}