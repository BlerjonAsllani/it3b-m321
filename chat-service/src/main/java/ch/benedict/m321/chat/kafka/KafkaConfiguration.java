package ch.benedict.m321.chat.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Legt fest, wie der chat-service mit Kafka spricht.
 *
 * Ein Topic ist ein fortlaufendes Protokoll (ein "Log"), in das geschrieben wird.
 * Jede Consumer-Gruppe, die es liest, bekommt eine EIGENE vollstaendige Kopie und
 * merkt sich selbst, wie weit sie gekommen ist. Genau das brauchen wir: eine Kopie
 * fuer jede chat-service-Instanz (Anzeige) und eine fuer den batch-service (Speichern).
 */
@Configuration
public class KafkaConfiguration {

    /** Name des Topics, auf das jede neue Nachricht geschrieben wird. */
    public static final String TOPIC_NAME = "chat.messages";

    /**
     * Wie viele Partitionen das Topic bekommt. Eine Partition ist ein Teilstueck des
     * Logs; Kafka garantiert die Reihenfolge nur INNERHALB einer Partition. Weil wir
     * die roomId als Schluessel senden, landen alle Nachrichten eines Raums in
     * derselben Partition - und damit garantiert in Sendereihenfolge.
     *
     * Mehr Partitionen erlauben spaeter mehr parallele Leser. 6 ist ein Startwert.
     */
    private static final int PARTITIONS = 6;

    /**
     * Meldet das Topic beim Broker an. Spring legt es beim Start automatisch an,
     * falls es noch nicht existiert - man muss auf der Kommandozeile nichts anlegen.
     *
     * replicas(1), weil in docker-compose genau ein Broker laeuft. Ein einzelner
     * Broker kann nichts replizieren; jede hoehere Zahl wuerde beim Anlegen scheitern.
     */
    @Bean
    public NewTopic chatTopic() {
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(PARTITIONS)
                .replicas(1)
                .build();
    }
}
