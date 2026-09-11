package ch.benedict.m321.chat.message;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testet MessageService allein, ohne Spring-Kontext und ohne Docker: KafkaTemplate und
 * MessageRepository sind Attrappen (Mockito), der ObjectMapper ist echt und genauso
 * eingestellt wie der, den Spring Boot dem Service normalerweise hereinreicht.
 */
class MessageServiceTest {

    private static final UUID ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    // .json().build() allein reicht NICHT: ohne Spring-Kontext bleibt
    // WRITE_DATES_AS_TIMESTAMPS auf dem Jackson-Standard (an) - genau das schaltet erst
    // Spring Boots JacksonAutoConfiguration ab. Das wird hier von Hand nachgestellt, damit
    // dieser ObjectMapper wie der echte, von Spring Boot injizierte, ISO-Text schreibt.
    private final ObjectMapper objectMapper = Jackson2ObjectMapperBuilder.json()
            .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private final MessageRepository messageRepository = mock(MessageRepository.class);

    private final MessageService messageService =
            new MessageService(kafkaTemplate, objectMapper, messageRepository);

    /**
     * Prueft, dass eine gesendete Nachricht mit der roomId als Kafka-Schluessel ankommt und
     * dass der Wert JSON mit allen Feldern ist, inklusive sentAt als lesbarem ISO-Zeitstempel.
     */
    @Test
    void sendMessageWritesJsonWithRoomIdAsKey() throws Exception {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("chat.messages", 0), 0L, 0, 0L, 0, 0);
        SendResult<String, String> result = new SendResult<>(null, metadata);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(result));

        NewMessage incoming = new NewMessage(ROOM_ID, "lernende1", "Hallo zusammen");
        messageService.sendMessage(incoming);

        // Wir fangen die drei Argumente von send() ab, um sie einzeln zu pruefen.
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), valueCaptor.capture());

        assertEquals("chat.messages", topicCaptor.getValue());
        assertEquals(ROOM_ID.toString(), keyCaptor.getValue());

        JsonNode json = objectMapper.readTree(valueCaptor.getValue());
        assertTrue(json.has("roomId"));
        assertTrue(json.has("sender"));
        assertTrue(json.has("text"));
        assertTrue(json.has("id"));
        assertTrue(json.get("sentAt").isTextual());
        assertTrue(json.get("sentAt").asText().endsWith("Z"));
    }

    /**
     * Prueft die 503-Antwort fuer den Fall, dass Kafka das Scheitern erst SPAETER ueber das
     * CompletableFuture meldet - send() selbst liefert normal zurueck.
     */
    @Test
    void sendMessageAnswers503WhenKafkaFailsLater() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Broker weg")));

        NewMessage incoming = new NewMessage(ROOM_ID, "lernende1", "Kommt das an?");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> messageService.sendMessage(incoming));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.getStatusCode());
    }

    /**
     * Prueft die 503-Antwort fuer den Fall, dass send() SOFORT eine KafkaException wirft (z. B.
     * weil der Producer noch keine Metadaten fuer das Topic hat) - der Service muss das genauso
     * abfangen wie ein spaeteres Scheitern, statt die Ausnahme unbehandelt bis zum Controller
     * durchzulassen (dort wuerde daraus ein HTTP 500 ohne WARN-Log).
     */
    @Test
    void sendMessageAnswers503WhenSendFailsImmediately() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("Send failed"));

        NewMessage incoming = new NewMessage(ROOM_ID, "lernende1", "Kommt das an?");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> messageService.sendMessage(incoming));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.getStatusCode());
    }
}
