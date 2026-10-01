package ch.benedict.m321.batch.message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Legt Nachrichten, die sich nie speichern lassen, auf das Dead-Letter-Topic. Dort kann man sie
 * anschauen - der Chat-Verlauf bekommt dadurch keine stillen Loecher, nur eine sichtbare Ablage.
 */
@Component
public class DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    /** Name des Kafka-Headers, in dem der Grund steht. */
    public static final String REASON_HEADER = "error-reason";

    /** So lange warten wir hoechstens auf die Bestaetigung von Kafka. */
    private static final int SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;

    /** Topic fuer nicht speicherbare Nachrichten; kommt aus der Konfiguration (dlq.topic). */
    private final String deadLetterTopicName;

    /** Spring reicht den KafkaTemplate und den Topic-Namen herein. */
    public DeadLetterPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               @Value("${dlq.topic}") String deadLetterTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterTopicName = deadLetterTopicName;
    }

    /**
     * Legt eine nicht speicherbare Nachricht unveraendert auf das Dead-Letter-Topic, mit dem Grund
     * als Header. Kehrt erst zurueck, wenn Kafka den Empfang bestaetigt hat - erst dann darf der
     * Listener die Nachricht als erledigt bestaetigen.
     */
    public void publish(ConsumerRecord<String, String> original, String reason) {
        log.warn("Nachricht aus {} Partition {} Offset {} geht auf {}: {}",
                original.topic(), original.partition(), original.offset(),
                deadLetterTopicName, reason);

        // Schluessel und Wert bleiben genau so, wie sie ankamen - damit man die Nachricht spaeter
        // unveraendert anschauen oder nach einer Korrektur erneut einspielen kann.
        ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                deadLetterTopicName, original.key(), original.value());
        byte[] reasonAsBytes = reason.getBytes(StandardCharsets.UTF_8);
        deadLetter.headers().add(REASON_HEADER, reasonAsBytes);

        // send() arbeitet im Hintergrund, in einem Thread des Kafka-Clients, und liefert nur ein
        // "Versprechen" (CompletableFuture). Auf das Ergebnis warten wir gleich darunter.
        CompletableFuture<SendResult<String, String>> pending = kafkaTemplate.send(deadLetter);
        waitForKafka(pending, original);
    }

    /**
     * Wartet auf die Bestaetigung von Kafka. Scheitert sie, wird das laut gemeldet und eine
     * Ausnahme fliegt weiter: der Listener bestaetigt das Paket dann nicht, Spring wiederholt es -
     * die Nachricht geht nicht verloren. Ohne die Meldung saehe man diesen Ausfall gar nicht, weil
     * "Datenbank weg" (Klasse 3) denselben Datensatz sonst still weiterreicht.
     */
    private void waitForKafka(CompletableFuture<SendResult<String, String>> pending,
                               ConsumerRecord<String, String> original) {
        try {
            pending.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            logPublishFailure(original);
            throw new IllegalStateException("Dead-Letter-Topic hat nicht bestaetigt", failure);
        } catch (InterruptedException interrupted) {
            // Der Thread wurde beim Warten unterbrochen, zum Beispiel beim Herunterfahren.
            // Wir setzen die Unterbrechungs-Markierung wieder, damit Spring davon erfaehrt.
            Thread.currentThread().interrupt();
            logPublishFailure(original);
            throw new IllegalStateException("Warten auf das Dead-Letter-Topic wurde unterbrochen", interrupted);
        }
    }

    /** Meldet laut, welcher Original-Datensatz nicht auf das Dead-Letter-Topic abgelegt werden konnte. */
    private void logPublishFailure(ConsumerRecord<String, String> original) {
        log.error("Nachricht aus {} Partition {} Offset {} konnte nicht auf {} abgelegt werden - Paket wird wiederholt",
                original.topic(), original.partition(), original.offset(),
                deadLetterTopicName);
    }
}
