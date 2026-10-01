package ch.benedict.m321.batch.message;

import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Wandelt den JSON-Text eines Topic-Eintrags in eine ChatMessage um. Die einzige Stelle im
 * batch-service, die das JSON-Format kennt.
 */
@Component
public class MessageParser {

    /**
     * Spaetester Zeitpunkt, den die Datenbank noch speichern kann. Ein spaeterer Zeitpunkt ist
     * gueltiges JSON und ein gueltiger Instant, aber Timestamp.from() wirft dafuer eine Ausnahme -
     * ohne diese Pruefung wuerde die Nachricht endlos wiederholt, obwohl sie nie speicherbar ist.
     */
    private static final Instant LATEST_STORABLE_TIME = Instant.parse("9999-12-31T23:59:59Z");

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

        // Ein fehlender Zeitpunkt (null) bleibt erlaubt - das lehnt spaeter die Datenbank ab
        // (Klasse 2, siehe MessageWriterIntegrationTest). Ein zu spaeter Zeitpunkt waere dagegen
        // fuer immer kaputt: Timestamp.from() wirft dafuer eine ArithmeticException, und ohne
        // diese Pruefung wuerde die Nachricht als "Datenbank weg" endlos wiederholt.
        if (message.sentAt() != null && message.sentAt().isAfter(LATEST_STORABLE_TIME)) {
            throw new InvalidMessageException(
                    "Zeitpunkt liegt nach dem Jahr 9999 und kann nicht gespeichert werden: " + message.sentAt());
        }
        return message;
    }
}
