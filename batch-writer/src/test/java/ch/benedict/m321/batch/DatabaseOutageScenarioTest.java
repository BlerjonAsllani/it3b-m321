package ch.benedict.m321.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PauseContainerCmd;
import com.github.dockerjava.api.command.UnpauseContainerCmd;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
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
 * Szenario S7 (Datenbankausfall): faehrt den ganzen batch-writer mit einem echten Kafka-Broker
 * und einer echten Datenbank hoch (beide startet der Test selbst in Docker), haelt die
 * Datenbank an und startet sie wieder. Das zeigt, dass der batch-writer ohne Neustart
 * weiterschreibt und keine Nachricht verliert. Die beiden Zeiten in properties sind nur fuer
 * diesen Test verkuerzt, damit er nicht eine halbe Minute auf den Ausfall warten muss.
 */
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(properties = {
        // Nach 5 statt 30 Sekunden Stille bricht der Datenbanktreiber ab (Vorgabe: application.yml).
        "spring.datasource.hikari.data-source-properties.socketTimeout=5",
        // Spring wiederholt das Paket nach 2 statt 5 Sekunden (Vorgabe: application.yml).
        "batch.retry-interval-ms=2000"
})
class DatabaseOutageScenarioTest {

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

    /** Der KafkaTemplate des batch-writer - der Test benutzt ihn, um Nachrichten zu senden. */
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Prueft S7: waehrend die Datenbank angehalten ist, werden drei Nachrichten gesendet. Der
     * Test wartet, bis der batch-writer den Datenbankfehler gemeldet hat, und startet die
     * Datenbank dann wieder. Danach muessen alle drei Zeilen in der Tabelle stehen - ohne dass
     * jemand den batch-writer neu startet.
     */
    @Test
    void messagesSentDuringDatabaseOutageArriveAfterwards(CapturedOutput output) throws Exception {
        // Erst eine Nachricht bei laufender Datenbank: so ist der Listener sicher bereit und der
        // Verbindungspool offen, bevor der Ausfall beginnt.
        UUID readyId = UUID.randomUUID();
        kafkaTemplate.send(chatTopic, DEMO_ROOM_ID, messageJson(readyId)).get();
        waitUntilStored(readyId);

        List<UUID> outageIds = new ArrayList<>();
        pauseDatabase();
        try {
            for (int number = 1; number <= 3; number++) {
                UUID id = UUID.randomUUID();
                outageIds.add(id);
                // get() wartet auf den Broker - Kafka laeuft ja weiter, nur die Datenbank nicht.
                kafkaTemplate.send(chatTopic, DEMO_ROOM_ID, messageJson(id)).get();
            }
            waitUntilDatabaseErrorIsReported(output);
        } finally {
            // Auch wenn oben etwas scheitert: die Datenbank muss wieder laufen, sonst bleibt der
            // Container eingefroren.
            unpauseDatabase();
        }

        for (UUID id : outageIds) {
            waitUntilStored(id);
        }
        for (UUID id : outageIds) {
            assertEquals(1, countRows(id));
        }
    }

    /**
     * Haelt den Datenbank-Container an (wie "docker pause"): alle Prozesse darin stehen still,
     * Postgres antwortet nicht mehr, aber die Verbindungen bleiben offen. Stoppen und neu
     * starten ginge nicht, weil der Container dann einen neuen Port bekaeme.
     */
    private void pauseDatabase() {
        DockerClient dockerClient = postgres.getDockerClient();
        String containerId = postgres.getContainerId();
        PauseContainerCmd pauseCommand = dockerClient.pauseContainerCmd(containerId);
        pauseCommand.exec();
    }

    /** Setzt den angehaltenen Datenbank-Container fort (wie "docker unpause"), mit demselben Port. */
    private void unpauseDatabase() {
        DockerClient dockerClient = postgres.getDockerClient();
        String containerId = postgres.getContainerId();
        UnpauseContainerCmd unpauseCommand = dockerClient.unpauseContainerCmd(containerId);
        unpauseCommand.exec();
    }

    /**
     * Wartet, bis der batch-writer den Ausfall im Log gemeldet hat (MessageListener schreibt dann
     * "Datenbankfehler"). Erst dann ist sicher, dass er die Datenbank wirklich nicht erreicht hat
     * und die Nachrichten nur durch Wiederholen ankommen koennen.
     */
    private void waitUntilDatabaseErrorIsReported(CapturedOutput output) {
        Callable<Boolean> isReported = () -> output.getAll().contains("Datenbankfehler");
        await().atMost(Duration.ofSeconds(30)).until(isReported);
    }

    /** Baut gueltiges Nachrichten-JSON fuer den Demo-Raum mit der angegebenen ID. */
    private String messageJson(UUID id) {
        return String.format("{\"id\":\"%s\",\"roomId\":\"%s\","
                + "\"sender\":\"lernende1\",\"text\":\"Hallo\","
                + "\"sentAt\":\"2026-09-11T08:05:00Z\"}", id, DEMO_ROOM_ID);
    }

    /**
     * Wartet bis zu 60 Sekunden, bis die Nachricht in der Tabelle steht. Kurz nach dem Fortsetzen
     * der Datenbank kann die allererste Abfrage des Tests noch an einer toten Verbindung
     * scheitern - ignoreExceptions() fragt dann einfach nochmals.
     */
    private void waitUntilStored(UUID id) {
        Callable<Boolean> isStored = () -> countRows(id) == 1;
        await().atMost(Duration.ofSeconds(60)).ignoreExceptions().until(isStored);
    }

    /** Zaehlt, wie viele Zeilen mit dieser ID in der Tabelle message stehen. */
    private int countRows(UUID id) {
        String sql = "SELECT count(*) FROM message WHERE id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, id);
        return count;
    }
}
