package com.example.pharmacy;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
@WebMvcTest(SaleController.class)
class SaleControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean OrderProducer producer;
    @Test void successRequiresBrokerAcknowledgement() throws Exception {
        when(producer.send(any())).thenAnswer(invocation -> {
            OrderEvent event = invocation.getArgument(0);
            return CompletableFuture.completedFuture(new SendResult<>(
                new ProducerRecord<>("medicine-stock-events", event.medicineId(), event),
                new RecordMetadata(new TopicPartition("medicine-stock-events", 2), 0, 0, 0, 0, 0)));
        });
        mvc.perform(post("/api/sales").contentType("application/json")
            .content("{\"medicineId\":\"MED-001\",\"quantity\":2}"))
            .andExpect(status().isCreated()).andExpect(jsonPath("$.partition").value(2))
            .andExpect(jsonPath("$.event.medicineId").value("MED-001"))
            .andExpect(jsonPath("$.event.orderId").isNotEmpty())
            .andExpect(jsonPath("$.event.timestamp").isNotEmpty());
    }
    @Test void rejectsInvalidInputWithoutSending() throws Exception {
        mvc.perform(post("/api/sales").contentType("application/json")
            .content("{\"medicineId\":\" \",\"quantity\":0}"))
            .andExpect(status().isBadRequest());
        verifyNoInteractions(producer);
    }
    @Test void failedSendMustNotReportSuccess() throws Exception {
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("offline")));
        mvc.perform(post("/api/sales").contentType("application/json")
            .content("{\"medicineId\":\"MED-001\",\"quantity\":1}"))
            .andExpect(status().isServiceUnavailable());
    }
}
