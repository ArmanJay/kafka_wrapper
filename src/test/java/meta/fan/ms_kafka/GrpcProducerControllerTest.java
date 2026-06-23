package meta.fan.ms_kafka;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import meta.fan.ms_kafka.controller.GrpcProducerController;
import meta.fan.ms_kafka.grpc.producer.ProduceRequest;
import meta.fan.ms_kafka.grpc.producer.ProduceResponse;
import meta.fan.ms_kafka.service.KafkaProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, SpringExtension.class})
@DisplayName("gRPC Producer Controller Tests")
class GrpcProducerControllerTest {

    @Mock
    private KafkaProducerService producerService;

    @Mock
    private SendResult<String, byte[]> sendResult;

    @Mock
    private org.apache.kafka.clients.producer.RecordMetadata recordMetadata;

    @Mock
    private StreamObserver<ProduceResponse> responseObserver;

    private GrpcProducerController grpcProducerController;

    @BeforeEach
    void setUp() {
        grpcProducerController = new GrpcProducerController(producerService);
    }

    @Test
    @DisplayName("Should send message via gRPC and return successful response")
    void testSendMessageSuccess() throws Exception {
        // Arrange
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = "test-payload".getBytes();

        ProduceRequest request = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setKey(key)
                .setPayload(ByteString.copyFrom(payload))
                .build();

        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        when(recordMetadata.partition()).thenReturn(0);
        when(recordMetadata.offset()).thenReturn(123L);
        when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);
        future.complete(sendResult);

        when(producerService.publish(topic, key, payload)).thenReturn(future);

        // Act
        grpcProducerController.sendMessage(request, responseObserver);

        // Wait a bit for async completion
        Thread.sleep(100);

        // Assert
        verify(responseObserver, times(1)).onNext(any(ProduceResponse.class));
        verify(responseObserver, times(1)).onCompleted();
        verify(producerService, times(1)).publish(topic, key, payload);
    }

    @Test
    @DisplayName("Should handle gRPC send message failure and return error response")
    void testSendMessageFailure() throws Exception {
        // Arrange
        String topic = "test-topic";
        String key = "test-key";
        byte[] payload = "test-payload".getBytes();
        String errorMessage = "Kafka broker unavailable";

        ProduceRequest request = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setKey(key)
                .setPayload(ByteString.copyFrom(payload))
                .build();

        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException(errorMessage));

        when(producerService.publish(topic, key, payload)).thenReturn(future);

        // Act
        grpcProducerController.sendMessage(request, responseObserver);

        // Wait a bit for async completion
        Thread.sleep(100);

        // Assert
        verify(responseObserver, times(1)).onNext(any(ProduceResponse.class));
        verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should handle empty topic gracefully")
    void testSendMessageWithEmptyTopic() throws Exception {
        // Arrange
        String topic = "";
        String key = "test-key";
        byte[] payload = "test-payload".getBytes();

        ProduceRequest request = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setKey(key)
                .setPayload(ByteString.copyFrom(payload))
                .build();

        CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalArgumentException("Topic name cannot be empty"));

        when(producerService.publish(topic, key, payload)).thenReturn(future);

        // Act
        grpcProducerController.sendMessage(request, responseObserver);

        // Wait a bit for async completion
        Thread.sleep(100);

        // Assert
        verify(responseObserver, times(1)).onNext(any(ProduceResponse.class));
        verify(responseObserver, times(1)).onCompleted();
    }
}
