package ch.benedict.m321.batch.message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import ch.benedict.m321.batch.kafka.KafkaConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    /** Spring reicht den KafkaTemplate herein; er schreibt Schluessel und Wert als Text. */
    public DeadLetterPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Legt eine nicht speicherbare Nachricht unveraendert auf das Dead-Letter-Topic, mit dem Grund
     * als Header. Kehrt erst zurueck, wenn Kafka den Empfang bestaetigt hat - erst dann darf der
     * Listener die Nachricht als erledigt bestaetigen.
     */
    public void publish(ConsumerRecord<String, String> original, String reason) {
        log.warn("Nachricht aus {} Partition {} Offset {} geht auf {}: {}",
                original.topic(), original.partition(), original.offset(),
                KafkaConfiguration.DEAD_LETTER_TOPIC_NAME, reason);

        // Schluessel und Wert bleiben genau so, wie sie ankamen - damit man die Nachricht spaeter
        // unveraendert anschauen oder nach einer Korrektur erneut einspielen kann.
        ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                KafkaConfiguration.DEAD_LETTER_TOPIC_NAME, original.key(), original.value());
        byte[] reasonAsBytes = reason.getBytes(StandardCharsets.UTF_8);
        deadLetter.headers().add(REASON_HEADER, reasonAsBytes);

        // send() arbeitet im Hintergrund, in einem Thread des Kafka-Clients, und liefert nur ein
        // "Versprechen" (CompletableFuture). Auf das Ergebnis warten wir gleich darunter.
        CompletableFuture<SendResult<String, String>> pending = kafkaTemplate.send(deadLetter);
        waitForKafka(pending);
    }

    /**
     * Wartet auf die Bestaetigung von Kafka. Scheitert sie, fliegt eine Ausnahme weiter: der
     * Listener bestaetigt das Paket dann nicht, Spring wiederholt es - die Nachricht geht nicht verloren.
     */
    private void waitForKafka(CompletableFuture<SendResult<String, String>> pending) {
        try {
            pending.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("Dead-Letter-Topic hat nicht bestaetigt", failure);
        } catch (InterruptedException interrupted) {
            // Der Thread wurde beim Warten unterbrochen, zum Beispiel beim Herunterfahren.
            // Wir setzen die Unterbrechungs-Markierung wieder, damit Spring davon erfaehrt.
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Warten auf das Dead-Letter-Topic wurde unterbrochen", interrupted);
        }
    }
}
