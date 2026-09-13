package ch.benedict.m321.batch.message;

import java.util.ArrayList;
import java.util.List;

import ch.benedict.m321.batch.kafka.KafkaConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
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

    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

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
     * Stufe 2: nimmt bis zu 500 Nachrichten auf einmal vom Topic und schreibt sie mit einem
     * einzigen batchUpdate. Bestaetigt wird erst, wenn jede Nachricht des Pakets entweder
     * gespeichert oder auf dem Dead-Letter-Topic abgelegt ist.
     */
    @KafkaListener(topics = KafkaConfiguration.TOPIC_NAME, groupId = KafkaConfiguration.GROUP_ID, batch = "true")
    public void onMessages(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        long startMillis = System.currentTimeMillis();

        // Zwei Listen mit gleichem Index: Stelle i gehoert in beiden zur selben Nachricht. Den
        // Originaleintrag brauchen wir, falls die Nachricht spaeter aufs Dead-Letter-Topic muss.
        List<ConsumerRecord<String, String>> validRecords = new ArrayList<>();
        List<ChatMessage> validMessages = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            try {
                ChatMessage message = messageParser.parse(record.value());
                validRecords.add(record);
                validMessages.add(message);
            } catch (InvalidMessageException broken) {
                // Klasse 1: kaputte Nachricht - kommt gar nicht erst ins INSERT.
                deadLetterPublisher.publish(record, broken.getMessage());
            }
        }

        try {
            writeBatch(validRecords, validMessages);
        } catch (DataAccessException problem) {
            // Klasse 3: die Datenbank hat gerade irgendein Problem - nicht erreichbar, zu
            // langsam (Fix A: socketTimeout) oder etwas anderes, das keine abgelehnte Zeile ist.
            // Egal ob beim gebuendelten Schreiben oder mitten im Einzelweg. Ablehnungen einzelner
            // Zeilen (Klasse 2) kommen hier nie an, die faengt writeBatch selbst.
            reportDatabaseProblem(problem, records.size());
            throw problem;
        }
        acknowledgment.acknowledge();

        long elapsedMillis = System.currentTimeMillis() - startMillis;
        log.info("Paket mit {} Nachrichten in {} ms geschrieben", validMessages.size(), elapsedMillis);
    }

    /**
     * Meldet einen Datenbankfehler laut, bevor er weitergeworfen wird. Wir werfen den Fehler
     * weiter: Spring wiederholt dann das ganze Paket, bestaetigt wird nichts. Ohne diese Meldung
     * liefe die Wiederholung still ab, und man saehe den Ausfall nur am wachsenden Lag.
     */
    private void reportDatabaseProblem(DataAccessException problem, int recordCount) {
        Throwable databaseError = problem.getMostSpecificCause();
        String databaseMessage = databaseError.getMessage();
        log.error("Datenbankfehler ({}: {}) - Paket mit {} Nachrichten wird wiederholt",
                problem.getClass().getSimpleName(), databaseMessage, recordCount);
    }

    /**
     * Schreibt das Paket gebuendelt. Lehnt die Datenbank eine einzige Zeile ab, scheitert das
     * ganze Paket - dann schreiben wir es einmal Zeile fuer Zeile, damit nur die schuldige Zeile
     * aufs Dead-Letter-Topic geht und alle anderen gespeichert werden.
     */
    private void writeBatch(List<ConsumerRecord<String, String>> validRecords, List<ChatMessage> validMessages) {
        // Waren alle Nachrichten kaputt, gibt es nichts zu schreiben.
        if (validMessages.isEmpty()) {
            return;
        }
        try {
            messageWriter.insertBatch(validMessages);
        } catch (DataIntegrityViolationException rejected) {
            // Klasse 2 im Paket: mindestens eine Zeile wird nie speicherbar sein. Andere Fehler
            // (Klasse 3, Datenbank weg) fangen wir hier bewusst NICHT - sie gehen an onMessages.
            String reason = rejectionReason(rejected);
            log.warn("Paket mit {} Nachrichten abgelehnt ({}) - schreibe Zeile fuer Zeile",
                    validMessages.size(), reason);
            writeOneByOne(validRecords, validMessages);
        }
    }

    /**
     * Einzelweg nach einem abgelehnten Paket. Zeilen, die vor dem Fehler schon geschrieben wurden,
     * ueberspringt ON CONFLICT - das Wiederholen ist deshalb ungefaehrlich.
     */
    private void writeOneByOne(List<ConsumerRecord<String, String>> validRecords, List<ChatMessage> validMessages) {
        for (int index = 0; index < validMessages.size(); index++) {
            ChatMessage message = validMessages.get(index);
            ConsumerRecord<String, String> record = validRecords.get(index);
            try {
                messageWriter.insertOne(message);
            } catch (DataIntegrityViolationException rejected) {
                String reason = rejectionReason(rejected);
                deadLetterPublisher.publish(record, reason);
            }
        }
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
