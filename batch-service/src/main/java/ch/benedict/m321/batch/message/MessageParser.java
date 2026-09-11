package ch.benedict.m321.batch.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Wandelt den JSON-Text eines Topic-Eintrags in eine ChatMessage um. Die einzige Stelle im
 * batch-service, die das JSON-Format kennt.
 */
@Component
public class MessageParser {

    private final ObjectMapper objectMapper;

    /**
     * Spring reicht seinen ObjectMapper herein. Der liest ISO-Zeitpunkte wie
     * "2026-09-11T08:05:00Z" und ignoriert Felder, die wir nicht kennen.
     */
    public MessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Liest eine Nachricht aus dem JSON-Text. Fehlende Felder bleiben null - ob das erlaubt
     * ist, entscheidet spaeter die Datenbank. Kaputtes JSON wird als InvalidMessageException gemeldet.
     */
    public ChatMessage parse(String json) throws InvalidMessageException {
        // Ein Kafka-Eintrag darf leer sein (Wert null) - eine Nachricht ist das aber nicht.
        if (json == null) {
            throw new InvalidMessageException("Eintrag ohne Inhalt");
        }

        ChatMessage message;
        try {
            message = objectMapper.readValue(json, ChatMessage.class);
        } catch (JsonProcessingException broken) {
            // getOriginalMessage() liefert nur den Grund, ohne Jacksons lange Positionsangaben.
            String reason = broken.getOriginalMessage();
            throw new InvalidMessageException("Ungueltiges JSON: " + reason);
        }

        // Das JSON-Wort null ist gueltiges JSON, liefert aber kein Objekt.
        if (message == null) {
            throw new InvalidMessageException("JSON enthaelt keine Nachricht");
        }
        return message;
    }
}
