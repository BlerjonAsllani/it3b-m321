package ch.benedict.m321.batch;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Szenario S5 (Duplikat): faehrt den ganzen batch-writer mit einem echten Kafka-Broker und einer
 * echten Datenbank hoch (beide startet der Test selbst in Docker) und legt dieselbe Nachricht
 * zweimal auf das Topic chat.persist. Das zeigt, dass ON CONFLICT im echten Zusammenspiel
 * funktioniert - nicht nur im SQL-Test gegen die Datenbank allein.
 */
@Testcontainers
@SpringBootTest
class DuplicateScenarioTest {

    /** Der Demo-Raum aus db/02-demo-data.sql - die Nachrichten brauchen einen bestehenden Raum. */
    private static final String DEMO_ROOM_ID = "11111111-1111-1111-1111-111111111111";

    /**
     * Die Datenbank fuer diese Testklasse, aufgebaut wie in MessageWriterIntegrationTest: die
     * beiden SQL-Dateien aus db/ legt das Postgres-Image beim ersten Start selbst an.
     * @ServiceConnection setzt die Adresse des Containers als Datenbankverbindung von Spring.
     */
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withCopyFileToContainer(MountableFile.forHostPath("../db/01-schema.sql"),
                    "/docker-entrypoint-initdb.d/01-schema.sql")
            .withCopyFileToContainer(MountableFile.forHostPath("../db/02-demo-data.sql"),
                    "/docker-entrypoint-initdb.d/02-demo-data.sql");

    /**
     * Der Kafka-Broker fuer diese Testklasse, mit demselben Image wie in docker-compose.
     * @ServiceConnection setzt seine Adresse als bootstrap-servers von Spring.
     */
    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");

    /** Name des Topics, das der Listener liest (chat.topic aus application.yml). */
    @Value("${chat.topic}")
    private String chatTopic;

    /** Name des Dead-Letter-Topics (dlq.topic aus application.yml). */
    @Value("${dlq.topic}")
    private String deadLetterTopic;

    /** Der KafkaTemplate des batch-writer - der Test benutzt ihn, um Nachrichten zu senden. */
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Prueft S5: dieselbe Nachricht zweimal gesendet ergibt genau eine Zeile und keinen Eintrag
     * auf dem Dead-Letter-Topic. Die zweite Nachricht wird erst gesendet, nachdem die erste
     * gespeichert ist - so sieht der Listener sie in einem eigenen Paket, wie bei einer
     * Wiederholung des chat-service. Eine Schlussnachricht danach zeigt, dass auch das Duplikat
     * verarbeitet ist: Kafka liefert die Nachrichten einer Partition in der Reihenfolge, in
     * der sie gesendet wurden.
     */
    @Test
    void sameMessageSentTwiceIsStoredOnce() throws Exception {
        UUID duplicateId = UUID.randomUUID();
        String duplicateJson = messageJson(duplicateId);
        UUID lastId = UUID.randomUUID();
        String lastJson = messageJson(lastId);

        // get() wartet, bis der Broker den Empfang bestaetigt hat.
        kafkaTemplate.send(chatTopic, DEMO_ROOM_ID, duplicateJson).get();
        waitUntilStored(duplicateId);

        kafkaTemplate.send(chatTopic, DEMO_ROOM_ID, duplicateJson).get();
        kafkaTemplate.send(chatTopic, DEMO_ROOM_ID, lastJson).get();
        waitUntilStored(lastId);

        assertEquals(1, countRows(duplicateId));
        assertEquals(0, countDeadLetters());
    }

    /** Baut gueltiges Nachrichten-JSON fuer den Demo-Raum mit der angegebenen ID. */
    private String messageJson(UUID id) {
        return String.format("{\"id\":\"%s\",\"roomId\":\"%s\","
                + "\"sender\":\"lernende1\",\"text\":\"Hallo\","
                + "\"sentAt\":\"2026-09-11T08:05:00Z\"}", id, DEMO_ROOM_ID);
    }

    /**
     * Wartet bis zu 30 Sekunden, bis die Nachricht in der Tabelle steht. Der Listener schreibt im
     * Hintergrund; Awaitility fragt die Datenbank alle paar Millisekunden, bis die Bedingung
     * stimmt, und laesst den Test sonst nach Ablauf der Zeit scheitern.
     */
    private void waitUntilStored(UUID id) {
        Callable<Boolean> isStored = () -> countRows(id) == 1;
        await().atMost(Duration.ofSeconds(30)).until(isStored);
    }

    /** Zaehlt, wie viele Zeilen mit dieser ID in der Tabelle message stehen. */
    private int countRows(UUID id) {
        String sql = "SELECT count(*) FROM message WHERE id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, id);
        return count;
    }

    /**
     * Zaehlt, wie viele Nachrichten je auf dem Dead-Letter-Topic gelandet sind. Dafuer fragt ein
     * eigener Consumer nur nach dem Ende der einzigen Partition: bei einem frischen Topic ist
     * das Ende (end offset) genau die Anzahl der Nachrichten darauf.
     */
    private long countDeadLetters() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // try-with-resources schliesst den Consumer am Ende wieder.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            TopicPartition partition = new TopicPartition(deadLetterTopic, 0);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(partition));
            Long endOffset = endOffsets.get(partition);
            return endOffset;
        }
    }
}
