package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Prueft das SQL gegen eine echte PostgreSQL-Datenbank, die der Test selbst in Docker startet
 * (Testcontainers) - der Test braucht also keinen laufenden docker-compose-Stack. @JdbcTest
 * startet nur den Datenbankteil von Spring (kein Kafka) und rollt nach jedem Test alles zurueck.
 * Replace.NONE heisst: keine eingebaute Test-Datenbank verwenden, sondern die echte aus dem Container.
 */
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
@Import(MessageWriter.class)
class MessageWriterIntegrationTest {

    /** Der Demo-Raum aus db/02-demo-data.sql - er existiert in jeder frischen Datenbank. */
    private static final UUID DEMO_ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    /**
     * Die Datenbank fuer diese Testklasse: ein PostgreSQL-Container, den Testcontainers vor dem
     * ersten Test startet und nach dem letzten wieder loescht. Die beiden SQL-Dateien aus db/
     * werden in den Ordner kopiert, den das Postgres-Image beim ersten Start selbst ausfuehrt -
     * genau wie in docker-compose. Der Pfad beginnt mit "..", weil Maven im Ordner batch-writer
     * laeuft und db/ eine Ebene darueber liegt. @ServiceConnection traegt die Adresse des
     * Containers als Datenbankverbindung in Spring ein, die URL aus application.yml gilt nicht.
     */
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withCopyFileToContainer(MountableFile.forHostPath("../db/01-schema.sql"),
                    "/docker-entrypoint-initdb.d/01-schema.sql")
            .withCopyFileToContainer(MountableFile.forHostPath("../db/02-demo-data.sql"),
                    "/docker-entrypoint-initdb.d/02-demo-data.sql");

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
