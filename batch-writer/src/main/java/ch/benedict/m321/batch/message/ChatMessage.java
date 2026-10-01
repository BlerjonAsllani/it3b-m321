package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.UUID;

/**
 * Eine Nachricht, so wie sie als JSON auf dem Topic chat.messages steht. Bewusst eine eigene
 * Kopie und nicht die Klasse aus dem chat-service: die beiden Dienste teilen nur das
 * Datenformat, keinen Code - jeder kann seine Klasse aendern, solange das JSON gleich bleibt.
 */
public record ChatMessage(
        UUID id,
        UUID roomId,
        String sender,
        String text,
        Instant sentAt) {
}
