package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Prueft das SQL gegen die echte Datenbank aus docker-compose. @JdbcTest startet nur den
 * Datenbankteil von Spring (kein Kafka) und rollt nach jedem Test alles zurueck - die
 * Testzeilen bleiben also nicht in der Datenbank liegen. Replace.NONE heisst: die Datenbank
 * aus application.yml verwenden, keine eingebaute Test-Datenbank.
 */
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(MessageWriter.class)
class MessageWriterIntegrationTest {

    /** Der Demo-Raum aus db/02-demo-data.sql - er existiert in jeder frischen Datenbank. */
    private static final UUID DEMO_ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @Autowired
    private MessageWriter messageWriter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /** Prueft, dass ein Paket mit einem einzigen Aufruf vollstaendig in der Tabelle landet. */
    @Test
    void insertBatchWritesAllRows() {
        ChatMessage first = messageFor(DEMO_ROOM_ID, "lernende1");
        ChatMessage second = messageFor(DEMO_ROOM_ID, "lernende2");
        List<ChatMessage> batch = List.of(first, second);

        messageWriter.insertBatch(batch);

        assertEquals(2, countRows(first.id(), second.id()));
    }

    /**
     * Prueft ON CONFLICT: liefert Kafka dasselbe Paket ein zweites Mal, entstehen keine
     * doppelten Zeilen und kein Fehler.
     */
    @Test
    void secondDeliveryOfTheSameBatchChangesNothing() {
        ChatMessage first = messageFor(DEMO_ROOM_ID, "lernende1");
        ChatMessage second = messageFor(DEMO_ROOM_ID, "lernende2");
        List<ChatMessage> batch = List.of(first, second);

        messageWriter.insertBatch(batch);
        messageWriter.insertBatch(batch);

        assertEquals(2, countRows(first.id(), second.id()));
    }

    /** Prueft, dass eine Nachricht fuer einen unbekannten Raum als abgelehnte Zeile gemeldet wird. */
    @Test
    void unknownRoomIsRejectedAsDataIntegrityViolation() {
        ChatMessage unknownRoom = messageFor(UUID.randomUUID(), "lernende1");

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertOne(unknownRoom));
    }

    /**
     * Prueft dasselbe fuer ein ganzes Paket: auch aus einem batchUpdate kommt die Ablehnung als
     * DataIntegrityViolationException - darauf baut der Einzelweg im Listener.
     */
    @Test
    void unknownRoomInABatchIsRejectedAsDataIntegrityViolation() {
        ChatMessage valid = messageFor(DEMO_ROOM_ID, "lernende1");
        ChatMessage unknownRoom = messageFor(UUID.randomUUID(), "lernende2");
        List<ChatMessage> batch = List.of(valid, unknownRoom);

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertBatch(batch));
    }

    /** Prueft, dass ein Absender ueber 100 Zeichen (VARCHAR(100)) als abgelehnte Zeile gemeldet wird. */
    @Test
    void tooLongSenderIsRejectedAsDataIntegrityViolation() {
        String tooLong = "x".repeat(101);
        ChatMessage message = messageFor(DEMO_ROOM_ID, tooLong);

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertOne(message));
    }

    /**
     * Prueft, dass ein fehlender Zeitpunkt von der Datenbank abgelehnt wird (NOT NULL) - und
     * nicht schon vorher im Code als NullPointerException abbricht, die der Listener faelschlich
     * fuer "Datenbank weg" halten wuerde.
     */
    @Test
    void missingTimestampIsRejectedAsDataIntegrityViolation() {
        ChatMessage withoutTimestamp = new ChatMessage(UUID.randomUUID(), DEMO_ROOM_ID, "lernende1", "Hallo", null);

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertOne(withoutTimestamp));
    }

    /** Baut eine gueltige Testnachricht mit neuer ID fuer den angegebenen Raum. */
    private ChatMessage messageFor(UUID roomId, String sender) {
        return new ChatMessage(UUID.randomUUID(), roomId, sender, "Testnachricht", Instant.now());
    }

    /** Zaehlt, wie viele der zwei IDs in der Tabelle stehen. */
    private int countRows(UUID firstId, UUID secondId) {
        String sql = "SELECT count(*) FROM message WHERE id IN (?, ?)";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, firstId, secondId);
        return count;
    }
}
