package meta.fan.ms_kafka.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaStreamsService {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsService.class);

    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {
        // 1. Read from the inbound topic
        KStream<String, String> orderStream = streamsBuilder.stream(
                "orders-inbound",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 2. Process the stream (Transform, Filter, Aggregate, etc.)
        orderStream
                .peek((key, value) -> log.info("Streams analyzing order: {}", value))
                // Example filter: Only keep strings that contain "VIP"
                .filter((key, value) -> value != null && value.contains("\"type\":\"VIP\""))
                // Transform the data (e.g., uppercase it)
                .mapValues(value -> value.toUpperCase())

                // 3. Write out to the sink topic
                .to("orders-vip-processed", Produced.with(Serdes.String(), Serdes.String()));

        log.info("Kafka Streams Topology Initialized: orders-inbound -> orders-vip-processed");
    }
}