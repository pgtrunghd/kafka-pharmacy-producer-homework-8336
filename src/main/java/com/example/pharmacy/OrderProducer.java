package com.example.pharmacy;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
@Service
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> template;
    private final String topic;
    public OrderProducer(KafkaTemplate<String, OrderEvent> template,
                         @Value("${app.order-topic}") String topic) {
        this.template = template;
        this.topic = topic;
    }
    public CompletableFuture<SendResult<String, OrderEvent>> send(OrderEvent event) {
        // Không chỉ định partition: Kafka băm medicineId để chọn partition.
        return template.send(topic, event.medicineId(), event);
    }
}
