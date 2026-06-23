package meta.fan.ms_kafka.config;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracerConfig {

    @Bean
    public Tracer tracer(io.micrometer.tracing.Tracer tracer) {
        // Spring Boot automatically provides a Tracer bean when micrometer-tracing is on classpath
        return tracer;
    }
}