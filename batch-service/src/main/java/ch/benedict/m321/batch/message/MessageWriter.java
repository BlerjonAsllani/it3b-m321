package ch.benedict.m321.batch.message;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Schreibt Nachrichten in die Tabelle message. Nur SQL, keine Entscheidungen: was bei einem
 * Fehler passiert, entscheidet der Listener. Der batch-service ist der einzige Dienst, der in
 * diese Tabelle schreibt (PLANUNG.md, Abschnitt 2.1).
 */
@Repository
public class MessageWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageWriter.class);

    /**
     * Dieselbe Anweisung fuer einzeln und gebuendelt. ON CONFLICT (id) DO NOTHING: liefert Kafka
     * eine Nachricht ein zweites Mal, wird sie still uebersprungen - die ID kommt vom
     * chat-service, deshalb erkennt die Datenbank die Wiederholung.
     */
    private static final String INSERT_SQL = "INSERT INTO message (id, room_id, sender, text, sent_at) "
                                           + "VALUES (?, ?, ?, ?, ?) "
                                           + "ON CONFLICT (id) DO NOTHING";

    private final JdbcTemplate jdbcTemplate;

    /** Spring reicht den JdbcTemplate herein (Konstruktor-Injektion). */
    public MessageWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Schreibt eine einzelne Nachricht: eine Anweisung, ein Roundtrip zur Datenbank, ein Commit.
     * Lehnt die Datenbank die Zeile ab, fliegt eine DataIntegrityViolationException.
     */
    public void insertOne(ChatMessage message) {
        Object[] values = toColumnValues(message);
        jdbcTemplate.update(INSERT_SQL, values);
        log.debug("Nachricht {} geschrieben", message.id());
    }

    /**
     * Schreibt viele Nachrichten mit einem einzigen batchUpdate. Dank reWriteBatchedInserts (in
     * den Hikari-data-source-properties, nicht in der URL) macht der Treiber daraus wenige
     * INSERT mit vielen Zeilen statt vieler einzelner Anweisungen - das ist der ganze Gewinn des
     * Buendelns.
     */
    public void insertBatch(List<ChatMessage> messages) {
        List<Object[]> allValues = new ArrayList<>();
        for (ChatMessage message : messages) {
            Object[] values = toColumnValues(message);
            allValues.add(values);
        }
        jdbcTemplate.batchUpdate(INSERT_SQL, allValues);
        log.debug("{} Nachrichten gebuendelt geschrieben", messages.size());
    }

    /**
     * Legt die Werte in der Reihenfolge der Fragezeichen im SQL ab. Fehlt der Zeitpunkt, bleibt
     * der Wert null - dann lehnt die Datenbank die Zeile ab (NOT NULL), statt dass der Code hier
     * mit einer NullPointerException abbricht.
     */
    private Object[] toColumnValues(ChatMessage message) {
        Timestamp sentAt = null;
        if (message.sentAt() != null) {
            sentAt = Timestamp.from(message.sentAt());
        }
        return new Object[] { message.id(), message.roomId(), message.sender(), message.text(), sentAt };
    }
}
