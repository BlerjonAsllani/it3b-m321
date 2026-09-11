package ch.benedict.m321.batch.message;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Testet die Einzelstufe des Listeners - je ein Test pro Fehlerklasse aus dem Design. Writer,
 * Dead-Letter-Publisher und Acknowledgment sind Attrappen; der Parser ist echt, damit kaputtes
 * JSON wirklich als kaputt erkannt wird.
 */
class MessageListenerTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final MessageParser messageParser = new MessageParser(objectMapper);
    private final MessageWriter messageWriter = mock(MessageWriter.class);
    private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);

    private final MessageListener listener =
            new MessageListener(messageParser, messageWriter, deadLetterPublisher);

    /** Prueft den Normalfall: gespeichert, bestaetigt, nichts auf dem Dead-Letter-Topic. */
    @Test
    void storesValidMessageAndAcknowledges() {
        ConsumerRecord<String, String> record = recordWithValue(0, validJson(1));

        listener.onMessage(record, acknowledgment);

        verify(messageWriter).insertOne(any());
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /** Klasse 1: kaputtes JSON geht aufs Dead-Letter-Topic, wird nicht geschrieben, aber bestaetigt. */
    @Test
    void brokenJsonGoesToDeadLetterTopicAndIsAcknowledged() {
        ConsumerRecord<String, String> record = recordWithValue(0, "{kaputt");

        listener.onMessage(record, acknowledgment);

        verify(deadLetterPublisher).publish(eq(record), anyString());
        verify(messageWriter, never()).insertOne(any());
        verify(acknowledgment).acknowledge();
    }

    /** Klasse 2: eine von der Datenbank abgelehnte Zeile geht aufs Dead-Letter-Topic und wird bestaetigt. */
    @Test
    void rejectedRowGoesToDeadLetterTopicAndIsAcknowledged() {
        ConsumerRecord<String, String> record = recordWithValue(0, validJson(1));
        doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .when(messageWriter).insertOne(any());

        listener.onMessage(record, acknowledgment);

        verify(deadLetterPublisher).publish(eq(record), startsWith("Von der Datenbank abgelehnt"));
        verify(acknowledgment).acknowledge();
    }

    /**
     * Klasse 3: ist die Datenbank nicht erreichbar, fliegt der Fehler weiter und es wird NICHT
     * bestaetigt - nur so wiederholt Spring die Nachricht, statt sie zu verlieren.
     */
    @Test
    void databaseDownIsPassedOnAndNotAcknowledged() {
        ConsumerRecord<String, String> record = recordWithValue(0, validJson(1));
        doThrow(new CannotGetJdbcConnectionException("Postgres weg"))
                .when(messageWriter).insertOne(any());

        assertThrows(CannotGetJdbcConnectionException.class, () -> listener.onMessage(record, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /** Baut gueltiges Nachrichten-JSON mit einer eigenen ID pro Nummer. */
    private String validJson(int number) {
        return String.format("{\"id\":\"aaaaaaaa-0000-0000-0000-%012d\","
                + "\"roomId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"sender\":\"lernende1\",\"text\":\"Hallo %d\","
                + "\"sentAt\":\"2026-09-11T08:05:00Z\"}", number, number);
    }

    /** Baut einen Topic-Eintrag, wie ihn Kafka dem Listener uebergibt. */
    private ConsumerRecord<String, String> recordWithValue(long offset, String value) {
        return new ConsumerRecord<>("chat.messages", 0, offset, "11111111-1111-1111-1111-111111111111", value);
    }
}
