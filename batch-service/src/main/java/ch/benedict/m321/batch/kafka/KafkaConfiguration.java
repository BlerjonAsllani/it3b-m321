package ch.benedict.m321.batch.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Legt die Kafka-Bausteine fest, die der batch-service selbst mitbringt: die Namen, das
 * Dead-Letter-Topic und die Fehlerbehandlung mit endloser Wiederholung.
 */
@Configuration
public class KafkaConfiguration {

    /** Hier liest der batch-service. Angelegt wird dieses Topic vom chat-service. */
    public static final String TOPIC_NAME = "chat.messages";

    /** Hier landen Nachrichten, die sich nie speichern lassen. */
    public static final String DEAD_LETTER_TOPIC_NAME = "chat.messages-dlt";

    /** Die dauerhafte Consumer-Gruppe. Unter diesem Namen merkt sich Kafka, wie weit wir gelesen haben. */
    public static final String GROUP_ID = "batch-service";

    /** Wartezeit zwischen zwei Versuchen, wenn die Datenbank nicht erreichbar ist. */
    private static final long RETRY_INTERVAL_MILLISECONDS = 5000;

    /**
     * Meldet das Dead-Letter-Topic an; Spring Boot legt es beim Start an, falls es fehlt.
     * Eine Partition reicht: hier landen nur die seltenen Nachrichten, die sich nie speichern lassen.
     */
    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(DEAD_LETTER_TOPIC_NAME)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * Fehlerbehandlung fuer alles, was der Listener weiterwirft - das ist nur noch "Infrastruktur
     * weg", alle anderen Fehler faengt er selbst. Von sich aus wuerde Spring nach wenigen
     * Versuchen aufgeben und das Paket still ueberspringen. Wir wiederholen stattdessen alle
     * 5 Sekunden, ohne Ende: lieber waechst der Lag sichtbar, als dass eine Nachricht verloren
     * geht (PLANUNG.md, Abschnitt 2.4). Spring Boot haengt diese Bean von selbst an den Listener.
     */
    @Bean
    public DefaultErrorHandler endlessRetryErrorHandler() {
        FixedBackOff everyFiveSeconds = new FixedBackOff(RETRY_INTERVAL_MILLISECONDS, FixedBackOff.UNLIMITED_ATTEMPTS);
        return new DefaultErrorHandler(everyFiveSeconds);
    }
}
