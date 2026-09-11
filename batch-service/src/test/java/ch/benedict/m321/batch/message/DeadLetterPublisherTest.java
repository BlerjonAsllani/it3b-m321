package ch.benedict.m321.batch.message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testet DeadLetterPublisher allein, ohne Spring und ohne Docker: der KafkaTemplate ist eine
 * Attrappe. ProducerRecord ist generisch, Mockito kann aber nur die rohe Klasse abfangen -
 * daher die unterdrueckten Warnungen.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class DeadLetterPublisherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private final DeadLetterPublisher publisher = new DeadLetterPublisher(kafkaTemplate);

    private final ConsumerRecord<String, String> original =
            new ConsumerRecord<>("chat.messages", 3, 42L, "11111111-1111-1111-1111-111111111111", "{kaputt");

    /**
     * Prueft, dass die Nachricht mit unveraendertem Schluessel und Wert auf dem Dead-Letter-Topic
     * landet und den Grund als Header traegt.
     */
    @Test
    void keepsKeyAndValueAndAddsReason() {
        CompletableFuture<SendResult<String, String>> confirmed = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(confirmed);

        publisher.publish(original, "Ungueltiges JSON");

        ArgumentCaptor<ProducerRecord> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<?, ?> sent = recordCaptor.getValue();

        assertEquals("chat.messages-dlt", sent.topic());
        assertEquals("11111111-1111-1111-1111-111111111111", sent.key());
        assertEquals("{kaputt", sent.value());

        Header reasonHeader = sent.headers().lastHeader("error-reason");
        String reason = new String(reasonHeader.value(), StandardCharsets.UTF_8);
        assertEquals("Ungueltiges JSON", reason);
    }

    /**
     * Prueft, dass ein Scheitern von Kafka nicht verschluckt wird: nur dann bestaetigt der
     * Listener das Paket nicht, und die Nachricht geht nicht verloren.
     */
    @Test
    void failureOfKafkaIsPassedOn() {
        CompletableFuture<SendResult<String, String>> failed =
                CompletableFuture.failedFuture(new RuntimeException("Broker weg"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        assertThrows(IllegalStateException.class, () -> publisher.publish(original, "Grund"));
    }
}
