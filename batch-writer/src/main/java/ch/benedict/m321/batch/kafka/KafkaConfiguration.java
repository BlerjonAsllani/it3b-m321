package ch.benedict.m321.batch.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Legt die Kafka-Bausteine fest, die der batch-writer selbst mitbringt: das Dead-Letter-Topic und
 * die Fehlerbehandlung mit endloser Wiederholung. Die Namen und die Wartezeit kommen aus der
 * Konfiguration, damit der Stack sie ohne Codeaenderung setzen kann.
 */
@Configuration
public class KafkaConfiguration {

    /** Hier landen Nachrichten, die sich nie speichern lassen (dlq.topic, Vorgabe chat.dlq). */
    private final String deadLetterTopicName;

    /** Wartezeit zwischen zwei Versuchen, wenn die Datenbank nicht erreichbar ist. */
    private final long retryIntervalMilliseconds;

    /** Spring reicht beide Werte aus der Konfiguration herein. */
    public KafkaConfiguration(@Value("${dlq.topic}") String deadLetterTopicName,
                              @Value("${batch.retry-interval-ms}") long retryIntervalMilliseconds) {
        this.deadLetterTopicName = deadLetterTopicName;
        this.retryIntervalMilliseconds = retryIntervalMilliseconds;
    }

    /**
     * Meldet das Dead-Letter-Topic an; Spring Boot legt es beim Start an, falls es fehlt.
     * Eine Partition reicht: hier landen nur die seltenen Nachrichten, die sich nie speichern lassen.
     */
    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetterTopicName)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * Fehlerbehandlung fuer alles, was der Listener weiterwirft - das ist nur noch "Infrastruktur
     * weg", alle anderen Fehler faengt er selbst. Von sich aus wuerde Spring nach wenigen
     * Versuchen aufgeben und das Paket still ueberspringen. Wir wiederholen stattdessen in festem
     * Abstand, ohne Ende: lieber waechst der Lag sichtbar, als dass eine Nachricht verloren geht
     * (PLANUNG.md, Abschnitt 2.4). Spring Boot haengt diese Bean von selbst an den Listener.
     */
    @Bean
    public DefaultErrorHandler endlessRetryErrorHandler() {
        FixedBackOff fixedPause = new FixedBackOff(retryIntervalMilliseconds, FixedBackOff.UNLIMITED_ATTEMPTS);
        return new DefaultErrorHandler(fixedPause);
    }
}
