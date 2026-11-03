package com.example.pharmacy;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
@RestController
@RequestMapping("/api/sales")
public class SaleController {
    private final OrderProducer producer;
    public SaleController(OrderProducer producer) { this.producer = producer; }
    @PostMapping
    public ResponseEntity<SaleResponse> sell(@Valid @RequestBody SaleRequest request) {
        OrderEvent event = new OrderEvent(UUID.randomUUID().toString(), request.medicineId(),
                request.quantity(), Instant.now());
        try {
            var metadata = producer.send(event).get(15, TimeUnit.SECONDS).getRecordMetadata();
            return ResponseEntity.status(HttpStatus.CREATED).body(new SaleResponse(
                    "Gửi thông tin thanh toán thành công", event, metadata.topic(),
                    metadata.partition(), metadata.offset()));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Gửi sự kiện bị gián đoạn", ex);
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Chưa xác nhận được sự kiện từ Kafka", ex);
        }
    }
    public record SaleResponse(String message, OrderEvent event, String topic, int partition, long offset) {}
}
