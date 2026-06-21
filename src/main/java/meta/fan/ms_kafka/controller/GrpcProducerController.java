package meta.fan.ms_kafka.controller;

import io.grpc.stub.StreamObserver;
import meta.fan.ms_kafka.grpc.producer.ProduceRequest;
import meta.fan.ms_kafka.grpc.producer.ProduceResponse;
import meta.fan.ms_kafka.grpc.producer.ProducerServiceGrpc;
import meta.fan.ms_kafka.service.KafkaProducerService;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class GrpcProducerController extends ProducerServiceGrpc.ProducerServiceImplBase {

    private final KafkaProducerService producerService;

    public GrpcProducerController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @Override
    public void sendMessage(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver) {
        producerService.publish(request.getTopic(), request.getKey(), request.getPayload().toByteArray())
                .thenAccept(result -> {
                    ProduceResponse response = ProduceResponse.newBuilder()
                            .setSuccess(true)
                            .setPartition(result.getRecordMetadata().partition())
                            .setOffset(result.getRecordMetadata().offset())
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                })
                .exceptionally(ex -> {
                    ProduceResponse response = ProduceResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage(ex.getMessage())
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return null;
                });
    }
}