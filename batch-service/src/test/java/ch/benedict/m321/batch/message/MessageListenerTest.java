package ch.benedict.m321.batch.message;

import java.util.List;

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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Testet die Paketstufe des Listeners - je ein Test pro Fehlerklasse aus dem Design, dazu der
 * Einzelweg. Writer, Dead-Letter-Publisher und Acknowledgment sind Attrappen; der Parser ist
 * echt, damit kaputtes JSON wirklich als kaputt erkannt wird.
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

    /** Prueft den Normalfall: das ganze Paket mit einem Aufruf geschrieben, dann bestaetigt. */
    @Test
    void writesWholeBatchAndAcknowledges() {
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                recordWithValue(1, validJson(2)),
                recordWithValue(2, validJson(3)));

        listener.onMessages(records, acknowledgment);

        verify(messageWriter).insertBatch(argThat(batch -> batch.size() == 3));
        verify(messageWriter, never()).insertOne(any());
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /** Klasse 1: kaputtes JSON geht aufs Dead-Letter-Topic und kommt gar nicht erst ins INSERT. */
    @Test
    void brokenJsonIsLeftOutOfTheBatch() {
        ConsumerRecord<String, String> broken = recordWithValue(1, "{kaputt");
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                broken,
                recordWithValue(2, validJson(3)));

        listener.onMessages(records, acknowledgment);

        verify(deadLetterPublisher).publish(eq(broken), anyString());
        verify(messageWriter).insertBatch(argThat(batch -> batch.size() == 2));
        verify(acknowledgment).acknowledge();
    }

    /**
     * Klasse 2 im Paket: lehnt die Datenbank eine Zeile ab, wird das Paket Zeile fuer Zeile
     * geschrieben - nur die schuldige Zeile geht aufs Dead-Letter-Topic, das Paket wird bestaetigt.
     */
    @Test
    void rejectedRowFallsBackToOneByOne() {
        ConsumerRecord<String, String> second = recordWithValue(1, validJson(2));
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                second,
                recordWithValue(2, validJson(3)));
        doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .when(messageWriter).insertBatch(anyList());
        // Einzeln geschrieben scheitert nur die zweite Nachricht.
        doNothing()
                .doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .doNothing()
                .when(messageWriter).insertOne(any());

        listener.onMessages(records, acknowledgment);

        verify(messageWriter, times(3)).insertOne(any());
        verify(deadLetterPublisher).publish(eq(second), startsWith("Von der Datenbank abgelehnt"));
        verify(deadLetterPublisher, times(1)).publish(any(), anyString());
        verify(acknowledgment).acknowledge();
    }

    /**
     * Klasse 3: ist die Datenbank nicht erreichbar, fliegt der Fehler weiter und es wird NICHT
     * bestaetigt - Spring wiederholt das ganze Paket, nichts geht aufs Dead-Letter-Topic.
     */
    @Test
    void databaseDownIsPassedOnAndNotAcknowledged() {
        List<ConsumerRecord<String, String>> records = List.of(recordWithValue(0, validJson(1)));
        doThrow(new CannotGetJdbcConnectionException("Postgres weg"))
                .when(messageWriter).insertBatch(anyList());

        assertThrows(CannotGetJdbcConnectionException.class, () -> listener.onMessages(records, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /**
     * Klasse 3 mitten im Einzelweg: faellt die Datenbank waehrend des Zeile-fuer-Zeile-Schreibens
     * aus, fliegt auch dieser Fehler weiter und das Paket bleibt unbestaetigt.
     */
    @Test
    void databaseDownDuringOneByOneIsPassedOn() {
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                recordWithValue(1, validJson(2)));
        doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .when(messageWriter).insertBatch(anyList());
        doThrow(new CannotGetJdbcConnectionException("Postgres weg"))
                .when(messageWriter).insertOne(any());

        assertThrows(CannotGetJdbcConnectionException.class, () -> listener.onMessages(records, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
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
