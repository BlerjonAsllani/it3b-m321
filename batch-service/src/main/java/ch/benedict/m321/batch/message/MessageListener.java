package ch.benedict.m321.batch.message;

import ch.benedict.m321.batch.kafka.KafkaConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Liest die Nachrichten vom Topic chat.messages und sorgt dafuer, dass jede entweder in der
 * Datenbank oder auf dem Dead-Letter-Topic landet. Entscheidet bei Fehlern, zu welcher der drei
 * Klassen sie gehoeren - SQL, JSON und das Dead-Letter-Topic erledigen andere Klassen.
 */
@Component
public class MessageListener {

    private final MessageParser messageParser;
    private final MessageWriter messageWriter;
    private final DeadLetterPublisher deadLetterPublisher;

    /** Spring reicht die drei Helfer herein (Konstruktor-Injektion). */
    public MessageListener(MessageParser messageParser,
                           MessageWriter messageWriter,
                           DeadLetterPublisher deadLetterPublisher) {
        this.messageParser = messageParser;
        this.messageWriter = messageWriter;
        this.deadLetterPublisher = deadLetterPublisher;
    }

    /**
     * Stufe 1: nimmt jede Nachricht einzeln vom Topic und schreibt sie einzeln. Bestaetigt wird
     * erst, wenn die Nachricht gespeichert oder auf dem Dead-Letter-Topic abgelegt ist.
     */
    @KafkaListener(topics = KafkaConfiguration.TOPIC_NAME, groupId = KafkaConfiguration.GROUP_ID)
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        ChatMessage message;
        try {
            message = messageParser.parse(record.value());
        } catch (InvalidMessageException broken) {
            // Klasse 1: kaputte Nachricht. Wird nie speicherbar - also ablegen und bestaetigen.
            deadLetterPublisher.publish(record, broken.getMessage());
            acknowledgment.acknowledge();
            return;
        }

        try {
            messageWriter.insertOne(message);
        } catch (DataIntegrityViolationException rejected) {
            // Klasse 2: die Datenbank lehnt genau diese Zeile ab (z. B. unbekannter Raum).
            // Alle anderen Fehler (Klasse 3, Datenbank weg) fangen wir bewusst NICHT: sie fliegen
            // weiter, es wird nicht bestaetigt, und Spring wiederholt die Nachricht alle 5 Sekunden.
            String reason = rejectionReason(rejected);
            deadLetterPublisher.publish(record, reason);
        }

        acknowledgment.acknowledge();
    }

    /** Macht aus der Ablehnung der Datenbank einen lesbaren Grund fuer den Dead-Letter-Header. */
    private String rejectionReason(DataIntegrityViolationException rejected) {
        // Die genaueste Ursache ist die Meldung von PostgreSQL selbst, z. B. welche
        // Fremdschluessel-Regel verletzt wurde - Springs eigene Meldung ist viel laenger.
        Throwable databaseError = rejected.getMostSpecificCause();
        String databaseMessage = databaseError.getMessage();
        return "Von der Datenbank abgelehnt: " + databaseMessage;
    }
}
