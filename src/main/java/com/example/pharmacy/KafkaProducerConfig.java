package com.example.pharmacy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
@Configuration
public class KafkaProducerConfig {
    @Bean
    ProducerFactory<String, OrderEvent> orderProducerFactory(KafkaProperties properties, ObjectMapper mapper) {
        var json = new JsonSerializer<OrderEvent>(mapper.copy()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS));
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(null),
                new StringSerializer(), json);
    }
    @Bean
    KafkaTemplate<String, OrderEvent> orderKafkaTemplate(ProducerFactory<String, OrderEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
