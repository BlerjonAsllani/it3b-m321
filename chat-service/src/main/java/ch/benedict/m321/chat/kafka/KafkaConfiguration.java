package ch.benedict.m321.chat.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Legt fest, wie der chat-service mit Kafka spricht.
 *
 * Ein Topic ist ein fortlaufendes Protokoll (ein "Log"), in das geschrieben wird.
 * Jede Consumer-Gruppe, die es liest, bekommt eine EIGENE vollstaendige Kopie und
 * merkt sich selbst, wie weit sie gekommen ist. Genau das brauchen wir: eine Kopie
 * fuer jede chat-service-Instanz (Anzeige) und eine fuer den batch-writer (Speichern).
 */
@Configuration
public class KafkaConfiguration {

    /**
     * Name des Topics, auf das jede neue Nachricht geschrieben wird. Kommt aus der
     * Konfiguration (chat.topic, im Compose aus der Umgebungsvariablen CHAT_TOPIC), damit der
     * Stack andere Namen bekommen kann, ohne dass eine Zeile Code geaendert wird.
     */
    private final String topicName;

    /**
     * Wie viele Partitionen das Topic bekommt. Eine Partition ist ein Teilstueck des
     * Logs; Kafka garantiert die Reihenfolge nur INNERHALB einer Partition. Weil wir
     * die roomId als Schluessel senden, landen alle Nachrichten eines Raums in
     * derselben Partition - und damit garantiert in Sendereihenfolge.
     *
     * Mehr Partitionen erlauben spaeter mehr parallele Leser. 6 ist ein Startwert.
     */
    private static final int PARTITIONS = 6;

    /** Spring reicht den Namen aus der Konfiguration herein. */
    public KafkaConfiguration(@Value("${chat.topic}") String topicName) {
        this.topicName = topicName;
    }

    /** Gibt den Topic-Namen an alle weiter, die auf dieses Topic schreiben. */
    public String topicName() {
        return topicName;
    }

    /**
     * Meldet das Topic beim Broker an. Spring legt es beim Start automatisch an,
     * falls es noch nicht existiert - man muss auf der Kommandozeile nichts anlegen.
     *
     * replicas(1), weil in docker-compose genau ein Broker laeuft. Ein einzelner
     * Broker kann nichts replizieren; jede hoehere Zahl wuerde beim Anlegen scheitern.
     */
    @Bean
    public NewTopic chatTopic() {
        return TopicBuilder.name(topicName)
                .partitions(PARTITIONS)
                .replicas(1)
                .build();
    }
}
