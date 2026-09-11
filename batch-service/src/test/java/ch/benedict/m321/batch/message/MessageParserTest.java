package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Testet MessageParser allein, ohne Spring und ohne Docker. Der ObjectMapper ist echt und so
 * eingestellt wie der von Spring Boot, soweit es fuers Lesen zaehlt: er kennt Instant
 * (JavaTimeModule) und ignoriert unbekannte Felder.
 */
class MessageParserTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final MessageParser parser = new MessageParser(objectMapper);

    /**
     * Prueft den Normalfall: JSON, wie es der chat-service schreibt, wird zu einer ChatMessage
     * mit allen Feldern - der ISO-Zeitpunkt wird dabei zum Instant.
     */
    @Test
    void parsesJsonWrittenByChatService() throws Exception {
        String json = """
                {"id":"aaaaaaaa-0000-0000-0000-000000000001","roomId":"11111111-1111-1111-1111-111111111111","sender":"lernende1","text":"Hallo","sentAt":"2026-09-11T08:05:00Z"}""";

        ChatMessage message = parser.parse(json);

        assertEquals(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"), message.id());
        assertEquals(UUID.fromString("11111111-1111-1111-1111-111111111111"), message.roomId());
        assertEquals("lernende1", message.sender());
        assertEquals("Hallo", message.text());
        assertEquals(Instant.parse("2026-09-11T08:05:00Z"), message.sentAt());
    }

    /** Prueft, dass kaputtes JSON als ungueltige Nachricht gemeldet wird. */
    @Test
    void rejectsBrokenJson() {
        assertThrows(InvalidMessageException.class, () -> parser.parse("{kein json"));
    }

    /** Prueft, dass ein Eintrag ganz ohne Inhalt als ungueltige Nachricht gemeldet wird. */
    @Test
    void rejectsEntryWithoutContent() {
        assertThrows(InvalidMessageException.class, () -> parser.parse(null));
    }

    /**
     * Prueft den Sonderfall des JSON-Worts null: gueltiges JSON, aber keine Nachricht. Ohne
     * diese Pruefung gaebe es spaeter eine NullPointerException im Listener.
     */
    @Test
    void rejectsJsonNull() {
        assertThrows(InvalidMessageException.class, () -> parser.parse("null"));
    }

    /**
     * Prueft, dass fehlende Felder null bleiben statt einen Fehler auszuloesen: ob eine Zeile
     * ohne Text erlaubt ist, entscheidet die Datenbank, nicht der Parser.
     */
    @Test
    void keepsMissingFieldsAsNull() throws Exception {
        ChatMessage message = parser.parse("{\"id\":\"aaaaaaaa-0000-0000-0000-000000000001\"}");

        assertNull(message.text());
        assertNull(message.sentAt());
    }
}
