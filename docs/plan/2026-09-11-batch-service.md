# batch-service — Implementierungsplan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Ziel:** Ein `batch-service`, der die Nachrichten vom Kafka-Topic `chat.messages` liest und in die
Tabelle `message` schreibt — zuerst einzeln, dann gebündelt, mit gemessenem Unterschied und einem
Dead-Letter-Topic für Nachrichten, die sich nie speichern lassen.

**Architektur:** Ein eigener Spring-Boot-Dienst ohne Weboberfläche. Ein `@KafkaListener` der
Consumer-Gruppe `batch-service` liest die Nachrichten, `MessageParser` wandelt den JSON-Text in einen
`record`, `MessageWriter` schreibt mit `JdbcTemplate`. Fehler fallen in drei Klassen: kaputte
Nachricht und von der Datenbank abgelehnte Zeile gehen auf `chat.messages-dlt`, «Datenbank weg» wird
endlos alle 5 Sekunden wiederholt. Bestätigt (Offset gespeichert) wird erst, wenn jede Nachricht
entweder in der Datenbank oder auf dem Dead-Letter-Topic liegt.

**Tech-Stack:** Java 21 · Spring Boot 3.5.16 · Spring for Apache Kafka · Spring JDBC (`JdbcTemplate`) ·
Jackson (`spring-boot-starter-json`) · PostgreSQL 17 · Apache Kafka 4 (KRaft) · Maven · Bash + awk
für das Lastskript

**Spec:** `docs/superpowers/specs/2026-09-11-batch-service-design.md` — und darüber `PLANUNG.md`
(Abschnitte 2.1, 2.3, 2.4, 3)

---

## Globale Vorgaben

Diese Punkte gelten für **jede** Aufgabe in diesem Plan.

| Vorgabe | Wert |
|---|---|
| Java | **21** — nicht ein neueres System-JDK; `java -version` muss 21 zeigen |
| Spring Boot | **3.5.16** (`spring-boot-starter-parent`) — gleich wie der `chat-service` |
| Basis-Paket | `ch.benedict.m321.batch` |
| **Sprache im Code** | **Englisch.** Klassen, Methoden, Variablen, Felder, Testmethoden, Tabellen- und Spaltennamen, JSON-Felder, Header-Namen — alles englisch. |
| **Sprache in Text** | **Deutsch.** Kommentare, Javadoc, Log-Ausgaben, Gründe im Dead-Letter-Header, Dokumente. |
| Codestil | `CLAUDE.md`: eine Anweisung pro Zeile, keine Stream-Ketten, keine Annotation-Magie, kurze Methoden, über jeder Methode ein bis zwei Sätze auf Deutsch |
| Kein Lombok | wie im `chat-service` |
| Logging | `INFO` pro Paket, `DEBUG` pro Zeile, `WARN` pro Nachricht aufs Dead-Letter-Topic |
| Abhängigkeiten | Nur was im Plan steht. Keine zusätzliche Bibliothek ohne Rückfrage — insbesondere **kein** Testcontainers |
| Kein geteilter Code | Der `batch-service` kopiert nichts aus dem `chat-service` und hängt nicht von ihm ab. Gemeinsam ist nur das JSON-Format auf dem Topic. |
| Laufzeit | Beide Dienste laufen in dieser Phase auf dem Host; `docker compose up -d` muss für Tests und Start laufen |

> **Wenn auf eurem Rechner schon ein Postgres auf Port 5432 läuft:** dann erreicht `localhost:5432`
> den falschen Server. Abhilfe: in einer **nicht committeten** `docker-compose.override.yml` den
> Port des Containers umlegen (z. B. `127.0.0.1:5433:5432`) und die URL per Umgebungsvariable
> überschreiben — **mit** dem Zusatz für das Bündeln:
> `SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/chat?reWriteBatchedInserts=true`.
> Fehlt `?reWriteBatchedInserts=true`, läuft alles, aber die Paketstufe ist langsamer als nötig
> und die Messung falsch.

### Was dieser Plan **nicht** enthält

- **Mitgliedsprüfung beim Senden** — Teilprojekt 2. Bis dahin nimmt der `chat-service` Nachrichten für
  jeden Raum an; der `batch-service` legt die für unbekannte Räume aufs Dead-Letter-Topic.
- **Archivierung und Statistik** (`@Scheduled`) — Teilprojekt 6.
- **Lag-Schwelle mit `503` im `chat-service`** (`PLANUNG.md`, offener Punkt 4) — bleibt offen.
- **Betrieb im Compose** — Teilprojekt 4.

---

## Dateistruktur

```
it3b-m321/
├── docker-compose.yml                                   ändern — Kafka-Volume, keine automatischen Topics
├── scripts/
│   └── load-test.sh                                     neu — Lastskript für die Messung
├── docs/
│   └── messungen/
│       └── <datum>-einzeln-vs-paket.md                  neu — Messergebnisse
├── PLANUNG.md                                           ändern — Korrekturen und Nachtrag
├── README.md                                            ändern — Stand, Dokumente, Ausprobieren
└── batch-service/
    ├── pom.xml                                          neu
    └── src/
        ├── main/
        │   ├── java/ch/benedict/m321/batch/
        │   │   ├── BatchServiceApplication.java         Startpunkt
        │   │   ├── kafka/
        │   │   │   └── KafkaConfiguration.java          Namen, Dead-Letter-Topic, endlose Wiederholung
        │   │   └── message/
        │   │       ├── ChatMessage.java                 Nachricht, wie sie als JSON auf dem Topic steht
        │   │       ├── InvalidMessageException.java     «das ist kein gültiges Nachrichten-JSON»
        │   │       ├── MessageParser.java               JSON-Text → ChatMessage
        │   │       ├── MessageWriter.java               SQL: insertOne, insertBatch
        │   │       ├── DeadLetterPublisher.java         nicht speicherbare Nachricht → chat.messages-dlt
        │   │       └── MessageListener.java             @KafkaListener, Stufe 1 einzeln / Stufe 2 gebündelt
        │   └── resources/
        │       └── application.yml
        └── test/java/ch/benedict/m321/batch/
            ├── BatchServiceApplicationTest.java         Kontext startet, Kafka-Beans da
            └── message/
                ├── MessageParserTest.java
                ├── MessageWriterIntegrationTest.java    gegen das echte Postgres, mit Rollback
                ├── DeadLetterPublisherTest.java
                └── MessageListenerTest.java
```

**Warum diese Aufteilung:** wie im `chat-service` ein Paket pro Fachthema (`message`, `kafka`). Jede
Klasse hat genau eine Aufgabe — der Listener entscheidet, was mit einer Nachricht passiert, kennt
aber weder SQL noch JSON noch das Dead-Letter-Topic im Detail. So lässt sich jede Klasse einzeln
testen und im Unterricht am Stück lesen.

---

## Task 1: Kafka dauerhaft machen — Volume und keine automatischen Topics

**Dateien:**
- Ändern: `docker-compose.yml`

**Schnittstellen:**
- Braucht: den laufenden Compose-Stapel aus dem Bootstrap (`m321-postgres`, `m321-kafka`).
- Liefert: Kafka speichert Topics und Offsets auf dem Volume `kafka-data`; Topics entstehen nur noch
  über `NewTopic`-Beans (`chat.messages` im `chat-service`, `chat.messages-dlt` im `batch-service`).

> **Warum zuerst.** Bisher liegen Topics und Offsets **im** Kafka-Container. `docker compose down`
> löscht den Container und damit alles — auch die Stelle, bis zu der der `batch-service` gelesen
> hat. Die Aussage «nach einem Neustart macht er dort weiter, wo er aufgehört hat» wäre dann nur
> bis zum nächsten `down` wahr.
>
> **Einmaliger Verlust:** beim Umstellen auf das Volume sind die bisherigen Topics weg. Das Topic
> `chat.messages` legt der `chat-service` beim nächsten Start neu an. Die Nachrichten, die bisher nur
> im Topic standen, waren ohnehin nie gespeichert — es gab ja noch keinen `batch-service`.

- [ ] **Schritt 1: Prüfen, dass der Kafka-Benutzer in den Datenordner schreiben darf**

Das Image `apache/kafka` läuft nicht als `root`. Ein neues Volume übernimmt Besitzer und Rechte des
Ordners, auf den es im Image gelegt wird — der Ordner muss also dem Kafka-Benutzer gehören.

```bash
docker run --rm --entrypoint sh apache/kafka:4.0.0 -c 'id; ls -ld /var/lib/kafka/data'
```

Erwartet: `uid=1000(appuser)` und `drwxrwxr-x ... appuser root ... /var/lib/kafka/data`.

- [ ] **Schritt 2: `docker-compose.yml` ändern**

Den Kopfkommentar ersetzen durch:

```yaml
# Infrastruktur: Datenbank und Message Broker.
# chat-service und batch-service laufen in dieser Phase noch auf dem Host (aus der IDE).
```

Im Dienst `kafka` am Ende von `environment:` diese Zeilen ergänzen:

```yaml
      # Topics entstehen nur ausdruecklich, ueber die NewTopic-Beans im chat-service und
      # im batch-service. Ohne diese Zeile legt Kafka ein unbekanntes Topic still selbst an,
      # sobald ein Client danach fragt - mit Standardwerten (1 Partition) statt unseren 6.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      # Hier speichert Kafka Topics und die Offsets der Consumer-Gruppen. Der Ordner liegt
      # auf dem Volume kafka-data - so ueberlebt beides ein "docker compose down".
      KAFKA_LOG_DIRS: /var/lib/kafka/data
```

Direkt unter dem `environment:`-Block des Dienstes `kafka` (auf gleicher Einrückung wie `ports:`):

```yaml
    volumes:
      - kafka-data:/var/lib/kafka/data
```

Und unten im Block `volumes:` das neue Volume anmelden:

```yaml
volumes:
  postgres-data:
  kafka-data:
```

- [ ] **Schritt 3: Stapel neu starten und Einstellungen prüfen**

```bash
cd it3b-m321
docker compose up -d
docker volume ls | grep kafka-data
docker exec m321-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 1 --describe --all | grep auto.create.topics.enable
```

Erwartet: `m321-kafka` wird neu erstellt, das Volume `…_kafka-data` existiert, und
`auto.create.topics.enable=false`. Kafka braucht nach dem Start 10–30 Sekunden; bis dahin mit
`kafka-topics.sh --list` wiederholen.

- [ ] **Schritt 4: Beweisen, dass ein Topic `docker compose down` überlebt**

```bash
docker exec m321-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic m321.volume-check --partitions 1 --replication-factor 1
docker compose down
docker compose up -d
```

Warten, bis Kafka antwortet, dann:

```bash
docker exec m321-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Erwartet: `m321.volume-check` steht noch in der Liste. Danach wieder löschen:

```bash
docker exec m321-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic m321.volume-check
```

- [ ] **Schritt 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(infra): Kafka mit Volume und ohne automatisch angelegte Topics"
```

---

## Task 2: Projekt anlegen — Kontext startet, Dead-Letter-Topic und Fehlerbehandlung vorhanden

**Dateien:**
- Erstellen: `batch-service/pom.xml`
- Erstellen: `batch-service/src/main/resources/application.yml`
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/BatchServiceApplication.java`
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/kafka/KafkaConfiguration.java`
- Test: `batch-service/src/test/java/ch/benedict/m321/batch/BatchServiceApplicationTest.java`

**Schnittstellen:**
- Braucht aus Task 1: Kafka ohne automatische Topics.
- Liefert für spätere Tasks:
  - `KafkaConfiguration.TOPIC_NAME` = `"chat.messages"`
  - `KafkaConfiguration.DEAD_LETTER_TOPIC_NAME` = `"chat.messages-dlt"`
  - `KafkaConfiguration.GROUP_ID` = `"batch-service"`
  - Bean `NewTopic deadLetterTopic()` und Bean `DefaultErrorHandler endlessRetryErrorHandler()`

- [ ] **Schritt 1: `batch-service/pom.xml` anlegen**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <!-- Wie beim chat-service: der Eltern-POM von Spring Boot legt die Versionen aller
         Spring-Bibliotheken fest, deshalb steht unten meist keine Versionsnummer. -->
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.16</version>
        <relativePath/>
    </parent>

    <groupId>ch.benedict.m321</groupId>
    <artifactId>batch-service</artifactId>
    <version>0.1.0</version>
    <name>batch-service</name>
    <description>Liest Nachrichten aus Kafka und speichert sie gebuendelt in PostgreSQL (Modul M321)</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <!-- Kein Web-Starter: der batch-service hat keine Weboberflaeche. -->

        <!-- JdbcTemplate: SQL im Klartext, auch fuer das gebuendelte batchUpdate. -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>

        <!-- Jackson samt dem ObjectMapper von Spring Boot, der ISO-Zeitpunkte liest. Der
             chat-service bekommt ihn ueber den Web-Starter mit; der batch-service hat keinen
             und braucht ihn deshalb ausdruecklich. -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-json</artifactId>
        </dependency>

        <!-- Treiber fuer PostgreSQL. Wird nur zur Laufzeit gebraucht. -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Spring Kafka: Listener, Acknowledgment, KafkaTemplate fuer das Dead-Letter-Topic. -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Baut ein ausfuehrbares JAR und erlaubt "mvn spring-boot:run" -->
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Schritt 2: `application.yml` anlegen**

Datei `batch-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: batch-service

  # Verbindung zur Datenbank aus docker-compose.
  # Zugangsdaten stehen hier im Klartext, weil das eine Uebungsumgebung ist.
  datasource:
    # reWriteBatchedInserts=true: ohne diesen Zusatz schickt der PostgreSQL-Treiber die
    # Zeilen eines batchUpdate trotzdem einzeln ueber die Leitung - das Buendeln waere umsonst.
    url: jdbc:postgresql://localhost:5432/chat?reWriteBatchedInserts=true
    username: chat
    password: chat
    hikari:
      # So lange wartet der Verbindungspool hoechstens auf eine Datenbankverbindung (Standard:
      # 30 Sekunden). Ist Postgres weg, merkt der Listener das so nach 5 statt nach 30 Sekunden.
      connection-timeout: 5000

  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      # Hat die Gruppe noch keine gespeicherte Position (allererster Start), liest sie das
      # Topic von Anfang an - auch Nachrichten, die gesendet wurden, bevor es den
      # batch-service gab.
      auto-offset-reset: earliest
      # Offsets speichert nur der Listener selbst, nach dem Schreiben in die Datenbank.
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # Die drei Werte zusammen naehern "500 Stueck oder 200 ms" an (PLANUNG.md 2.3).
      # Startwerte - nachgemessen mit scripts/load-test.sh.
      max-poll-records: 500
      fetch-max-wait: 200ms
      fetch-min-size: 100KB
    producer:
      # Nur fuer das Dead-Letter-Topic: Schluessel und Wert gehen unveraendert als Text weiter.
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        # So lange darf send() hoechstens blockieren, wenn der Broker nicht erreichbar ist.
        # Die eckigen Klammern sorgen dafuer, dass Spring die Punkte im Namen stehen laesst.
        "[max.block.ms]": 5000
    listener:
      # Ein Offset wird erst gespeichert, wenn der Listener acknowledge() aufruft.
      ack-mode: manual

logging:
  level:
    # INFO: eine Zeile pro Paket. Fuer jede einzelne Zeile auf DEBUG stellen - aber nicht
    # waehrend einer Messung: 100 000 Logzeilen verfaelschen das Ergebnis.
    ch.benedict.m321: INFO
```

- [ ] **Schritt 3: Den fehlschlagenden Test schreiben**

Datei `batch-service/src/test/java/ch/benedict/m321/batch/BatchServiceApplicationTest.java`:

```java
package ch.benedict.m321.batch;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Faehrt den ganzen batch-service hoch - aber mit angehaltenem Listener (auto-startup=false):
 * der Test soll pruefen, dass alle Teile zusammenpassen, und nicht nebenbei echte Nachrichten
 * vom Topic lesen und in die Datenbank schreiben.
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class BatchServiceApplicationTest {

    @Autowired
    private NewTopic deadLetterTopic;

    @Autowired
    private DefaultErrorHandler errorHandler;

    /**
     * Prueft, dass das Dead-Letter-Topic mit einer Partition und einem Replikat angemeldet ist
     * und dass es eine Fehlerbehandlung gibt, die Spring Boot an den Listener haengt.
     */
    @Test
    void kafkaBeansAreCreated() {
        assertEquals("chat.messages-dlt", deadLetterTopic.name());
        assertEquals(1, deadLetterTopic.numPartitions());
        assertEquals(1, deadLetterTopic.replicationFactor());
        assertNotNull(errorHandler);
    }
}
```

- [ ] **Schritt 4: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/batch-service
export JAVA_HOME=<Pfad-zu-deinem-JDK-21>   # nur nötig, wenn java -version nicht 21 zeigt
mvn test
```

Erwartet: **FEHLSCHLAG** mit `Unable to find a @SpringBootConfiguration`.

- [ ] **Schritt 5: Die Startklasse schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/BatchServiceApplication.java`:

```java
package ch.benedict.m321.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Startpunkt des batch-service. Er hat keine Weboberflaeche: nach dem Start laufen nur die
 * Threads des Kafka-Listeners - und die halten den Prozess am Leben, bis man ihn beendet.
 */
@SpringBootApplication
public class BatchServiceApplication {

    /**
     * Uebergibt die Startklasse an Spring Boot. Alles Weitere - Datenbankverbindung,
     * Kafka-Listener, Konfigurationsdateien - erledigt der Aufruf darunter.
     */
    public static void main(String[] args) {
        SpringApplication.run(BatchServiceApplication.class, args);
    }
}
```

- [ ] **Schritt 6: Test laufen lassen — er muss wieder fehlschlagen, jetzt aus anderem Grund**

```bash
mvn test
```

Erwartet: **FEHLSCHLAG** mit `NoSuchBeanDefinitionException` für
`org.apache.kafka.clients.admin.NewTopic` — der Kontext startet, aber es gibt noch kein
Dead-Letter-Topic.

- [ ] **Schritt 7: Die Kafka-Konfiguration schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/kafka/KafkaConfiguration.java`:

```java
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
```

- [ ] **Schritt 8: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 1, Failures: 0, Errors: 0`.

> **Docker muss laufen.** Beim Hochfahren legt Spring über den `KafkaAdmin` das Dead-Letter-Topic
> an; ohne Kafka meldet der Test Verbindungsfehler im Log.

- [ ] **Schritt 9: Prüfen, dass das Dead-Letter-Topic wirklich angelegt wurde**

```bash
docker exec m321-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic chat.messages-dlt
```

Erwartet: `PartitionCount: 1`, `ReplicationFactor: 1`.

- [ ] **Schritt 10: Commit**

```bash
cd it3b-m321
git add batch-service/
git commit -m "feat(batch-service): Projekt aufsetzen, Dead-Letter-Topic und endlose Wiederholung"
```

---

## Task 3: JSON lesen — `ChatMessage` und `MessageParser`

**Dateien:**
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/message/ChatMessage.java`
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/message/InvalidMessageException.java`
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageParser.java`
- Test: `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageParserTest.java`

**Schnittstellen:**
- Braucht aus Task 2: den `ObjectMapper` von Spring Boot (`spring-boot-starter-json`).
- Liefert für spätere Tasks:
  - `record ChatMessage(UUID id, UUID roomId, String sender, String text, Instant sentAt)`
  - `class InvalidMessageException extends Exception` mit Konstruktor `(String reason)`
  - `ChatMessage MessageParser.parse(String json) throws InvalidMessageException`

- [ ] **Schritt 1: Den fehlschlagenden Test schreiben**

Datei `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageParserTest.java`:

```java
package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Testet MessageParser allein, ohne Spring und ohne Docker. Der ObjectMapper ist echt und so
 * eingestellt wie der von Spring Boot, soweit es fuers Lesen zaehlt: er kennt Instant
 * (JavaTimeModule) und ignoriert unbekannte Felder.
 */
class MessageParserTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final MessageParser parser = new MessageParser(objectMapper);

    /**
     * Prueft den Normalfall: JSON, wie es der chat-service schreibt, wird zu einer ChatMessage
     * mit allen Feldern - der ISO-Zeitpunkt wird dabei zum Instant.
     */
    @Test
    void parsesJsonWrittenByChatService() throws Exception {
        String json = """
                {"id":"aaaaaaaa-0000-0000-0000-000000000001","roomId":"11111111-1111-1111-1111-111111111111","sender":"lernende1","text":"Hallo","sentAt":"2026-09-11T08:05:00Z"}""";

        ChatMessage message = parser.parse(json);

        assertEquals(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"), message.id());
        assertEquals(UUID.fromString("11111111-1111-1111-1111-111111111111"), message.roomId());
        assertEquals("lernende1", message.sender());
        assertEquals("Hallo", message.text());
        assertEquals(Instant.parse("2026-09-11T08:05:00Z"), message.sentAt());
    }

    /** Prueft, dass kaputtes JSON als ungueltige Nachricht gemeldet wird. */
    @Test
    void rejectsBrokenJson() {
        assertThrows(InvalidMessageException.class, () -> parser.parse("{kein json"));
    }

    /** Prueft, dass ein Eintrag ganz ohne Inhalt als ungueltige Nachricht gemeldet wird. */
    @Test
    void rejectsEntryWithoutContent() {
        assertThrows(InvalidMessageException.class, () -> parser.parse(null));
    }

    /**
     * Prueft den Sonderfall des JSON-Worts null: gueltiges JSON, aber keine Nachricht. Ohne
     * diese Pruefung gaebe es spaeter eine NullPointerException im Listener.
     */
    @Test
    void rejectsJsonNull() {
        assertThrows(InvalidMessageException.class, () -> parser.parse("null"));
    }

    /**
     * Prueft, dass fehlende Felder null bleiben statt einen Fehler auszuloesen: ob eine Zeile
     * ohne Text erlaubt ist, entscheidet die Datenbank, nicht der Parser.
     */
    @Test
    void keepsMissingFieldsAsNull() throws Exception {
        ChatMessage message = parser.parse("{\"id\":\"aaaaaaaa-0000-0000-0000-000000000001\"}");

        assertNull(message.text());
        assertNull(message.sentAt());
    }
}
```

- [ ] **Schritt 2: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/batch-service
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `MessageParser`, `ChatMessage` und
`InvalidMessageException` gibt es noch nicht.

- [ ] **Schritt 3: Den Datensatz `ChatMessage` schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/ChatMessage.java`:

```java
package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.UUID;

/**
 * Eine Nachricht, so wie sie als JSON auf dem Topic chat.messages steht. Bewusst eine eigene
 * Kopie und nicht die Klasse aus dem chat-service: die beiden Dienste teilen nur das
 * Datenformat, keinen Code - jeder kann seine Klasse aendern, solange das JSON gleich bleibt.
 */
public record ChatMessage(
        UUID id,
        UUID roomId,
        String sender,
        String text,
        Instant sentAt) {
}
```

- [ ] **Schritt 4: Die Ausnahme `InvalidMessageException` schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/InvalidMessageException.java`:

```java
package ch.benedict.m321.batch.message;

/**
 * Meldet, dass ein Eintrag auf dem Topic kein gueltiges Nachrichten-JSON ist. Eine gepruefte
 * Ausnahme (extends Exception): wer parse() aufruft, MUSS entscheiden, was mit einer kaputten
 * Nachricht passiert - der Compiler laesst ihn das nicht vergessen.
 */
public class InvalidMessageException extends Exception {

    /** Legt die Ausnahme mit einer deutschen Beschreibung des Problems an. */
    public InvalidMessageException(String reason) {
        super(reason);
    }
}
```

- [ ] **Schritt 5: Den `MessageParser` schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageParser.java`:

```java
package ch.benedict.m321.batch.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Wandelt den JSON-Text eines Topic-Eintrags in eine ChatMessage um. Die einzige Stelle im
 * batch-service, die das JSON-Format kennt.
 */
@Component
public class MessageParser {

    private final ObjectMapper objectMapper;

    /**
     * Spring reicht seinen ObjectMapper herein. Der liest ISO-Zeitpunkte wie
     * "2026-09-11T08:05:00Z" und ignoriert Felder, die wir nicht kennen.
     */
    public MessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Liest eine Nachricht aus dem JSON-Text. Fehlende Felder bleiben null - ob das erlaubt
     * ist, entscheidet spaeter die Datenbank. Kaputtes JSON wird als InvalidMessageException gemeldet.
     */
    public ChatMessage parse(String json) throws InvalidMessageException {
        // Ein Kafka-Eintrag darf leer sein (Wert null) - eine Nachricht ist das aber nicht.
        if (json == null) {
            throw new InvalidMessageException("Eintrag ohne Inhalt");
        }

        ChatMessage message;
        try {
            message = objectMapper.readValue(json, ChatMessage.class);
        } catch (JsonProcessingException broken) {
            // getOriginalMessage() liefert nur den Grund, ohne Jacksons lange Positionsangaben.
            String reason = broken.getOriginalMessage();
            throw new InvalidMessageException("Ungueltiges JSON: " + reason);
        }

        // Das JSON-Wort null ist gueltiges JSON, liefert aber kein Objekt.
        if (message == null) {
            throw new InvalidMessageException("JSON enthaelt keine Nachricht");
        }
        return message;
    }
}
```

- [ ] **Schritt 6: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 6, Failures: 0, Errors: 0` (1 aus Task 2, 5 neue).

- [ ] **Schritt 7: Commit**

```bash
cd it3b-m321
git add batch-service/
git commit -m "feat(batch-service): Nachrichten-JSON vom Topic lesen"
```

---

## Task 4: In die Datenbank schreiben — `MessageWriter`

**Dateien:**
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageWriter.java`
- Test: `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageWriterIntegrationTest.java`

**Schnittstellen:**
- Braucht aus Task 3: `ChatMessage`.
- Liefert für spätere Tasks:
  - `void MessageWriter.insertOne(ChatMessage message)`
  - `void MessageWriter.insertBatch(List<ChatMessage> messages)`
  - Beide werfen bei einer abgelehnten Zeile `org.springframework.dao.DataIntegrityViolationException`,
    bei nicht erreichbarer Datenbank eine andere `DataAccessException`
    (z. B. `CannotGetJdbcConnectionException`).

> **Warum ein Integrationstest und kein Mock.** Die ganze Fehlereinteilung des Listeners hängt an
> einer Annahme: «unbekannter Raum, zu langer Absender, fehlender Zeitpunkt» kommen als
> `DataIntegrityViolationException` an — auch aus einem `batchUpdate`. Das entscheidet Spring
> anhand des SQL-Fehlercodes von PostgreSQL. Ein Mock würde die Annahme nur wiederholen; prüfen
> kann sie nur die echte Datenbank.

- [ ] **Schritt 1: Den fehlschlagenden Test schreiben**

Datei `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageWriterIntegrationTest.java`:

```java
package ch.benedict.m321.batch.message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Prueft das SQL gegen die echte Datenbank aus docker-compose. @JdbcTest startet nur den
 * Datenbankteil von Spring (kein Kafka) und rollt nach jedem Test alles zurueck - die
 * Testzeilen bleiben also nicht in der Datenbank liegen. Replace.NONE heisst: die Datenbank
 * aus application.yml verwenden, keine eingebaute Test-Datenbank.
 */
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(MessageWriter.class)
class MessageWriterIntegrationTest {

    /** Der Demo-Raum aus db/02-demo-data.sql - er existiert in jeder frischen Datenbank. */
    private static final UUID DEMO_ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @Autowired
    private MessageWriter messageWriter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /** Prueft, dass ein Paket mit einem einzigen Aufruf vollstaendig in der Tabelle landet. */
    @Test
    void insertBatchWritesAllRows() {
        ChatMessage first = messageFor(DEMO_ROOM_ID, "lernende1");
        ChatMessage second = messageFor(DEMO_ROOM_ID, "lernende2");
        List<ChatMessage> batch = List.of(first, second);

        messageWriter.insertBatch(batch);

        assertEquals(2, countRows(first.id(), second.id()));
    }

    /**
     * Prueft ON CONFLICT: liefert Kafka dasselbe Paket ein zweites Mal, entstehen keine
     * doppelten Zeilen und kein Fehler.
     */
    @Test
    void secondDeliveryOfTheSameBatchChangesNothing() {
        ChatMessage first = messageFor(DEMO_ROOM_ID, "lernende1");
        ChatMessage second = messageFor(DEMO_ROOM_ID, "lernende2");
        List<ChatMessage> batch = List.of(first, second);

        messageWriter.insertBatch(batch);
        messageWriter.insertBatch(batch);

        assertEquals(2, countRows(first.id(), second.id()));
    }

    /** Prueft, dass eine Nachricht fuer einen unbekannten Raum als abgelehnte Zeile gemeldet wird. */
    @Test
    void unknownRoomIsRejectedAsDataIntegrityViolation() {
        ChatMessage unknownRoom = messageFor(UUID.randomUUID(), "lernende1");

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertOne(unknownRoom));
    }

    /**
     * Prueft dasselbe fuer ein ganzes Paket: auch aus einem batchUpdate kommt die Ablehnung als
     * DataIntegrityViolationException - darauf baut der Einzelweg im Listener.
     */
    @Test
    void unknownRoomInABatchIsRejectedAsDataIntegrityViolation() {
        ChatMessage valid = messageFor(DEMO_ROOM_ID, "lernende1");
        ChatMessage unknownRoom = messageFor(UUID.randomUUID(), "lernende2");
        List<ChatMessage> batch = List.of(valid, unknownRoom);

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertBatch(batch));
    }

    /** Prueft, dass ein Absender ueber 100 Zeichen (VARCHAR(100)) als abgelehnte Zeile gemeldet wird. */
    @Test
    void tooLongSenderIsRejectedAsDataIntegrityViolation() {
        String tooLong = "x".repeat(101);
        ChatMessage message = messageFor(DEMO_ROOM_ID, tooLong);

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertOne(message));
    }

    /**
     * Prueft, dass ein fehlender Zeitpunkt von der Datenbank abgelehnt wird (NOT NULL) - und
     * nicht schon vorher im Code als NullPointerException abbricht, die der Listener faelschlich
     * fuer "Datenbank weg" halten wuerde.
     */
    @Test
    void missingTimestampIsRejectedAsDataIntegrityViolation() {
        ChatMessage withoutTimestamp = new ChatMessage(UUID.randomUUID(), DEMO_ROOM_ID, "lernende1", "Hallo", null);

        assertThrows(DataIntegrityViolationException.class, () -> messageWriter.insertOne(withoutTimestamp));
    }

    /** Baut eine gueltige Testnachricht mit neuer ID fuer den angegebenen Raum. */
    private ChatMessage messageFor(UUID roomId, String sender) {
        return new ChatMessage(UUID.randomUUID(), roomId, sender, "Testnachricht", Instant.now());
    }

    /** Zaehlt, wie viele der zwei IDs in der Tabelle stehen. */
    private int countRows(UUID firstId, UUID secondId) {
        String sql = "SELECT count(*) FROM message WHERE id IN (?, ?)";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, firstId, secondId);
        return count;
    }
}
```

- [ ] **Schritt 2: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/batch-service
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `MessageWriter` gibt es noch nicht.

- [ ] **Schritt 3: Den `MessageWriter` schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageWriter.java`:

```java
package ch.benedict.m321.batch.message;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Schreibt Nachrichten in die Tabelle message. Nur SQL, keine Entscheidungen: was bei einem
 * Fehler passiert, entscheidet der Listener. Der batch-service ist der einzige Dienst, der in
 * diese Tabelle schreibt (PLANUNG.md, Abschnitt 2.1).
 */
@Repository
public class MessageWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageWriter.class);

    /**
     * Dieselbe Anweisung fuer einzeln und gebuendelt. ON CONFLICT (id) DO NOTHING: liefert Kafka
     * eine Nachricht ein zweites Mal, wird sie still uebersprungen - die ID kommt vom
     * chat-service, deshalb erkennt die Datenbank die Wiederholung.
     */
    private static final String INSERT_SQL = "INSERT INTO message (id, room_id, sender, text, sent_at) "
                                           + "VALUES (?, ?, ?, ?, ?) "
                                           + "ON CONFLICT (id) DO NOTHING";

    private final JdbcTemplate jdbcTemplate;

    /** Spring reicht den JdbcTemplate herein (Konstruktor-Injektion). */
    public MessageWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Schreibt eine einzelne Nachricht: eine Anweisung, ein Roundtrip zur Datenbank, ein Commit.
     * Lehnt die Datenbank die Zeile ab, fliegt eine DataIntegrityViolationException.
     */
    public void insertOne(ChatMessage message) {
        Object[] values = toColumnValues(message);
        jdbcTemplate.update(INSERT_SQL, values);
        log.debug("Nachricht {} geschrieben", message.id());
    }

    /**
     * Schreibt viele Nachrichten mit einem einzigen batchUpdate. Dank reWriteBatchedInserts in
     * der JDBC-URL macht der Treiber daraus wenige INSERT mit vielen Zeilen statt vieler
     * einzelner Anweisungen - das ist der ganze Gewinn des Buendelns.
     */
    public void insertBatch(List<ChatMessage> messages) {
        List<Object[]> allValues = new ArrayList<>();
        for (ChatMessage message : messages) {
            Object[] values = toColumnValues(message);
            allValues.add(values);
        }
        jdbcTemplate.batchUpdate(INSERT_SQL, allValues);
        log.debug("{} Nachrichten gebuendelt geschrieben", messages.size());
    }

    /**
     * Legt die Werte in der Reihenfolge der Fragezeichen im SQL ab. Fehlt der Zeitpunkt, bleibt
     * der Wert null - dann lehnt die Datenbank die Zeile ab (NOT NULL), statt dass der Code hier
     * mit einer NullPointerException abbricht.
     */
    private Object[] toColumnValues(ChatMessage message) {
        Timestamp sentAt = null;
        if (message.sentAt() != null) {
            sentAt = Timestamp.from(message.sentAt());
        }
        return new Object[] { message.id(), message.roomId(), message.sender(), message.text(), sentAt };
    }
}
```

- [ ] **Schritt 4: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 12, Failures: 0, Errors: 0` (6 bisher, 6 neue).

Danach prüfen, dass der Rollback gewirkt hat — keine Testzeilen in der Datenbank:

```bash
docker exec m321-postgres psql -U chat -d chat -c "SELECT count(*) FROM message WHERE text = 'Testnachricht';"
```

Erwartet: `0`.

- [ ] **Schritt 5: Commit**

```bash
cd it3b-m321
git add batch-service/
git commit -m "feat(batch-service): Nachrichten einzeln und gebuendelt schreiben"
```

---

## Task 5: Nicht speicherbare Nachrichten ablegen — `DeadLetterPublisher`

**Dateien:**
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/message/DeadLetterPublisher.java`
- Test: `batch-service/src/test/java/ch/benedict/m321/batch/message/DeadLetterPublisherTest.java`

**Schnittstellen:**
- Braucht aus Task 2: `KafkaConfiguration.DEAD_LETTER_TOPIC_NAME`, den `KafkaTemplate` von Spring Boot.
- Liefert für spätere Tasks:
  - `void DeadLetterPublisher.publish(ConsumerRecord<String, String> original, String reason)` —
    kehrt erst zurück, wenn Kafka bestätigt hat; wirft sonst `IllegalStateException` (oder eine
    `KafkaException` direkt aus `send`).
  - `DeadLetterPublisher.REASON_HEADER` = `"error-reason"`

- [ ] **Schritt 1: Den fehlschlagenden Test schreiben**

Datei `batch-service/src/test/java/ch/benedict/m321/batch/message/DeadLetterPublisherTest.java`:

```java
package ch.benedict.m321.batch.message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testet DeadLetterPublisher allein, ohne Spring und ohne Docker: der KafkaTemplate ist eine
 * Attrappe. ProducerRecord ist generisch, Mockito kann aber nur die rohe Klasse abfangen -
 * daher die unterdrueckten Warnungen.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class DeadLetterPublisherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private final DeadLetterPublisher publisher = new DeadLetterPublisher(kafkaTemplate);

    private final ConsumerRecord<String, String> original =
            new ConsumerRecord<>("chat.messages", 3, 42L, "11111111-1111-1111-1111-111111111111", "{kaputt");

    /**
     * Prueft, dass die Nachricht mit unveraendertem Schluessel und Wert auf dem Dead-Letter-Topic
     * landet und den Grund als Header traegt.
     */
    @Test
    void keepsKeyAndValueAndAddsReason() {
        CompletableFuture<SendResult<String, String>> confirmed = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(confirmed);

        publisher.publish(original, "Ungueltiges JSON");

        ArgumentCaptor<ProducerRecord> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<?, ?> sent = recordCaptor.getValue();

        assertEquals("chat.messages-dlt", sent.topic());
        assertEquals("11111111-1111-1111-1111-111111111111", sent.key());
        assertEquals("{kaputt", sent.value());

        Header reasonHeader = sent.headers().lastHeader("error-reason");
        String reason = new String(reasonHeader.value(), StandardCharsets.UTF_8);
        assertEquals("Ungueltiges JSON", reason);
    }

    /**
     * Prueft, dass ein Scheitern von Kafka nicht verschluckt wird: nur dann bestaetigt der
     * Listener das Paket nicht, und die Nachricht geht nicht verloren.
     */
    @Test
    void failureOfKafkaIsPassedOn() {
        CompletableFuture<SendResult<String, String>> failed =
                CompletableFuture.failedFuture(new RuntimeException("Broker weg"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        assertThrows(IllegalStateException.class, () -> publisher.publish(original, "Grund"));
    }
}
```

- [ ] **Schritt 2: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/batch-service
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `DeadLetterPublisher` gibt es noch nicht.

- [ ] **Schritt 3: Den `DeadLetterPublisher` schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/DeadLetterPublisher.java`:

```java
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
```

- [ ] **Schritt 4: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 14, Failures: 0, Errors: 0` (12 bisher, 2 neue).

- [ ] **Schritt 5: Commit**

```bash
cd it3b-m321
git add batch-service/
git commit -m "feat(batch-service): nicht speicherbare Nachrichten aufs Dead-Letter-Topic legen"
```

---

## Task 6: Stufe 1 — jede Nachricht einzeln speichern

**Dateien:**
- Erstellen: `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageListener.java`
- Test: `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageListenerTest.java`

**Schnittstellen:**
- Braucht aus Task 2–5: `KafkaConfiguration.TOPIC_NAME`, `KafkaConfiguration.GROUP_ID`,
  `MessageParser.parse`, `MessageWriter.insertOne`, `DeadLetterPublisher.publish`.
- Liefert: `@KafkaListener`-Methode
  `void MessageListener.onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)`.
  Wird in Task 8 durch die Paketstufe ersetzt.

- [ ] **Schritt 1: Den fehlschlagenden Test schreiben**

Datei `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageListenerTest.java`:

```java
package ch.benedict.m321.batch.message;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Testet die Einzelstufe des Listeners - je ein Test pro Fehlerklasse aus dem Design. Writer,
 * Dead-Letter-Publisher und Acknowledgment sind Attrappen; der Parser ist echt, damit kaputtes
 * JSON wirklich als kaputt erkannt wird.
 */
class MessageListenerTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final MessageParser messageParser = new MessageParser(objectMapper);
    private final MessageWriter messageWriter = mock(MessageWriter.class);
    private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);

    private final MessageListener listener =
            new MessageListener(messageParser, messageWriter, deadLetterPublisher);

    /** Prueft den Normalfall: gespeichert, bestaetigt, nichts auf dem Dead-Letter-Topic. */
    @Test
    void storesValidMessageAndAcknowledges() {
        ConsumerRecord<String, String> record = recordWithValue(0, validJson(1));

        listener.onMessage(record, acknowledgment);

        verify(messageWriter).insertOne(any());
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /** Klasse 1: kaputtes JSON geht aufs Dead-Letter-Topic, wird nicht geschrieben, aber bestaetigt. */
    @Test
    void brokenJsonGoesToDeadLetterTopicAndIsAcknowledged() {
        ConsumerRecord<String, String> record = recordWithValue(0, "{kaputt");

        listener.onMessage(record, acknowledgment);

        verify(deadLetterPublisher).publish(eq(record), anyString());
        verify(messageWriter, never()).insertOne(any());
        verify(acknowledgment).acknowledge();
    }

    /** Klasse 2: eine von der Datenbank abgelehnte Zeile geht aufs Dead-Letter-Topic und wird bestaetigt. */
    @Test
    void rejectedRowGoesToDeadLetterTopicAndIsAcknowledged() {
        ConsumerRecord<String, String> record = recordWithValue(0, validJson(1));
        doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .when(messageWriter).insertOne(any());

        listener.onMessage(record, acknowledgment);

        verify(deadLetterPublisher).publish(eq(record), startsWith("Von der Datenbank abgelehnt"));
        verify(acknowledgment).acknowledge();
    }

    /**
     * Klasse 3: ist die Datenbank nicht erreichbar, fliegt der Fehler weiter und es wird NICHT
     * bestaetigt - nur so wiederholt Spring die Nachricht, statt sie zu verlieren.
     */
    @Test
    void databaseDownIsPassedOnAndNotAcknowledged() {
        ConsumerRecord<String, String> record = recordWithValue(0, validJson(1));
        doThrow(new CannotGetJdbcConnectionException("Postgres weg"))
                .when(messageWriter).insertOne(any());

        assertThrows(CannotGetJdbcConnectionException.class, () -> listener.onMessage(record, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /** Baut gueltiges Nachrichten-JSON mit einer eigenen ID pro Nummer. */
    private String validJson(int number) {
        return String.format("{\"id\":\"aaaaaaaa-0000-0000-0000-%012d\","
                + "\"roomId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"sender\":\"lernende1\",\"text\":\"Hallo %d\","
                + "\"sentAt\":\"2026-09-11T08:05:00Z\"}", number, number);
    }

    /** Baut einen Topic-Eintrag, wie ihn Kafka dem Listener uebergibt. */
    private ConsumerRecord<String, String> recordWithValue(long offset, String value) {
        return new ConsumerRecord<>("chat.messages", 0, offset, "11111111-1111-1111-1111-111111111111", value);
    }
}
```

- [ ] **Schritt 2: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/batch-service
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `MessageListener` gibt es noch nicht.

- [ ] **Schritt 3: Den Listener (Stufe 1) schreiben**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageListener.java`:

```java
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
```

- [ ] **Schritt 4: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 18, Failures: 0, Errors: 0` (14 bisher, 4 neue).
`BatchServiceApplicationTest` läuft weiter mit angehaltenem Listener — er liest also noch nichts.

- [ ] **Schritt 5: Von Hand prüfen — eine gesendete Nachricht erscheint im Verlauf**

Drei Terminals, alle im Projektwurzelverzeichnis:

```bash
docker compose up -d
```

```bash
cd chat-service && mvn spring-boot:run
```

```bash
cd batch-service && mvn spring-boot:run
```

> **Der `chat-service` zuerst.** Seit Task 1 legt Kafka keine Topics mehr von selbst an. Das Topic
> `chat.messages` entsteht erst, wenn der `chat-service` startet. Startet der `batch-service` zuerst,
> meldet er so lange Warnungen (`UNKNOWN_TOPIC_OR_PARTITION`), bis es das Topic gibt — das ist harmlos.

Im Log des `batch-service` muss erscheinen, dass der Gruppe `batch-service` Partitionen zugeteilt
wurden (`partitions assigned`). Dann:

```bash
curl -s -i -X POST http://localhost:8080/api/messages -H 'Content-Type: application/json' \
  -d '{"roomId":"11111111-1111-1111-1111-111111111111","sender":"lernende1","text":"Jetzt wird gespeichert"}'
sleep 1
curl -s "http://localhost:8080/api/messages?roomId=11111111-1111-1111-1111-111111111111&limit=1"
```

Erwartet: `202`, danach liefert `GET` genau diese Nachricht als neueste. **Das ist der Moment, auf den
das ganze Modul hingearbeitet hat:** senden und speichern sind getrennte Dienste, verbunden nur über
Kafka.

- [ ] **Schritt 6: Von Hand prüfen — unbekannter Raum landet auf dem Dead-Letter-Topic**

Der `chat-service` prüft den Raum noch nicht (Teilprojekt 2) — er nimmt die Nachricht an:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8080/api/messages \
  -H 'Content-Type: application/json' \
  -d '{"roomId":"99999999-9999-9999-9999-999999999999","sender":"lernende1","text":"Falscher Raum"}'
docker exec m321-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic chat.messages-dlt --from-beginning --property print.headers=true --timeout-ms 10000
```

Erwartet: `202`; im Log des `batch-service` eine `WARN`-Zeile «… geht auf chat.messages-dlt: Von der
Datenbank abgelehnt: …foreign key…»; der Consumer zeigt den Eintrag mit
`error-reason:Von der Datenbank abgelehnt: …` vor dem JSON.

- [ ] **Schritt 7: Von Hand prüfen — nach einem Neustart macht er dort weiter, wo er aufgehört hat**

1. Den `batch-service` mit `Ctrl+C` beenden.
2. Zwei Nachrichten senden (wie in Schritt 5, Texte «Waehrend der Pause 1» und «… 2»).
3. `GET` wie in Schritt 5 mit `limit=3`: die zwei neuen Nachrichten fehlen — niemand speichert.
4. Den `batch-service` wieder starten, eine Sekunde warten, `GET` wiederholen.

Erwartet: jetzt stehen beide Nachrichten im Verlauf. Kafka hat sich die Position der Gruppe
`batch-service` gemerkt; nichts ging verloren und nichts wurde doppelt geschrieben.

Beide Dienste mit `Ctrl+C` beenden.

- [ ] **Schritt 8: Commit**

```bash
cd it3b-m321
git add batch-service/
git commit -m "feat(batch-service): Stufe 1 - jede Nachricht einzeln speichern"
```

---

## Task 7: Messen, Stufe 1 — Lastskript und erstes Ergebnis

**Dateien:**
- Erstellen: `scripts/load-test.sh`
- Erstellen: `docs/messungen/<datum>-einzeln-vs-paket.md` — `<datum>` ist der Tag der Messung,
  z. B. `2026-09-11-einzeln-vs-paket.md`

**Schnittstellen:**
- Braucht aus Task 6: den laufenden `batch-service` in Stufe 1.
- Liefert für Task 8: `scripts/load-test.sh <anzahl>` und die Messdatei mit der Zeile für Stufe 1.

- [ ] **Schritt 1: Das Lastskript anlegen**

Datei `scripts/load-test.sh`:

```bash
#!/usr/bin/env bash
# Lasttest fuer den batch-service (Modul M321).
#
# Schreibt <anzahl> gueltige Nachrichten direkt auf das Kafka-Topic chat.messages und misst,
# wie lange es dauert, bis der batch-service alle in PostgreSQL gespeichert hat.
# Direkt auf Kafka statt ueber POST /api/messages: so misst das Skript den batch-service und
# nicht die HTTP-Schicht des chat-service.
#
# Aufruf aus dem Projektwurzelverzeichnis (docker compose und batch-service muessen laufen):
#   scripts/load-test.sh 100000

set -euo pipefail

COUNT="${1:-100000}"
ROOM_ID="11111111-1111-1111-1111-111111111111"

# Acht Hex-Ziffern aus der aktuellen Sekunde machen die IDs jedes Laufs eindeutig. Sonst wuerde
# ein zweiter Lauf an ON CONFLICT abprallen und waere scheinbar sofort fertig.
RUN_ID=$(printf '%08x' "$(date +%s)")
SENT_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Zaehlt alle Zeilen der Tabelle message.
count_rows() {
  docker exec m321-postgres psql -U chat -d chat -tAc "SELECT count(*) FROM message"
}

# Summe des Lags ueber alle Partitionen: Spalte 6 der Ausgabe von kafka-consumer-groups.sh.
current_lag() {
  docker exec m321-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group batch-service 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'
}

BEFORE=$(count_rows)
TARGET=$((BEFORE + COUNT))
echo "Vorher in der Datenbank: $BEFORE Nachrichten. Ziel: $TARGET."

START=$(date +%s)

# awk erzeugt die Nachrichten viel schneller als eine Bash-Schleife. Jede Zeile ist
# "<schluessel><TAB><json>"; kafka-console-producer trennt am Tabulator Schluessel und Wert.
# Die ID hat das UUID-Format: 8 Hex-Ziffern des Laufs, dann die laufende Nummer.
awk -v count="$COUNT" -v run="$RUN_ID" -v room="$ROOM_ID" -v sentAt="$SENT_AT" 'BEGIN {
  for (i = 1; i <= count; i++) {
    printf "%s\t{\"id\":\"%s-0000-4000-8000-%012d\",\"roomId\":\"%s\",\"sender\":\"lasttest\",\"text\":\"Lastnachricht %d\",\"sentAt\":\"%s\"}\n", room, run, i, room, i, sentAt
  }
}' | docker exec -i m321-kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic chat.messages \
      --property parse.key=true --property "key.separator=$(printf '\t')"

SENT=$(date +%s)
echo "Alle $COUNT Nachrichten nach $((SENT - START)) s auf dem Topic. Warte auf die Datenbank ..."

LOOP=0
while true; do
  NOW_ROWS=$(count_rows)
  if [ "$NOW_ROWS" -ge "$TARGET" ]; then
    break
  fi
  # Den Lag nur jede vierte Runde abfragen: das Werkzeug braucht selbst ein bis zwei Sekunden
  # und wuerde sonst die Messung ungenau machen.
  if [ $((LOOP % 4)) -eq 0 ]; then
    echo "  nach $(( $(date +%s) - START )) s: $((NOW_ROWS - BEFORE)) von $COUNT gespeichert, Lag $(current_lag)"
  fi
  LOOP=$((LOOP + 1))
  sleep 0.5
done

END=$(date +%s)
DURATION=$((END - START))
if [ "$DURATION" -lt 1 ]; then
  DURATION=1
fi
echo "Fertig: $COUNT Nachrichten in $DURATION s, also etwa $((COUNT / DURATION)) Nachrichten pro Sekunde."
```

Ausführbar machen:

```bash
chmod +x scripts/load-test.sh
```

- [ ] **Schritt 2: Das Skript mit wenigen Nachrichten ausprobieren**

`chat-service` und `batch-service` (Stufe 1) laufen lassen, dann:

```bash
cd it3b-m321
scripts/load-test.sh 1000
```

Erwartet: «Fertig: 1000 Nachrichten in … s». Keine `WARN`-Zeile im `batch-service` — alle 1000 sind
gültig. Wenn der `batch-service` Warnungen zu kaputtem JSON meldet, stimmt der Tabulator als
Trennzeichen nicht.

- [ ] **Schritt 3: Stufe 1 messen**

Log-Level bleibt `INFO` (siehe `application.yml`). Dann:

```bash
scripts/load-test.sh 100000
```

Die letzte Zeile («Fertig: …») notieren. Dauert der Lauf länger als 15 Minuten, abbrechen und
**beide** Stufen mit `20000` messen — die Zahl steht dann in der Tabelle.

- [ ] **Schritt 4: Die Messdatei anlegen**

Datei `docs/messungen/<datum>-einzeln-vs-paket.md` — mit den echten Werten aus Schritt 3 und dem
echten Rechner:

```markdown
# Messung: einzeln gegen gebündelt

Gemessen am <datum> mit `scripts/load-test.sh 100000` gegen den `batch-service`.
Alles auf einem Rechner: <Rechner, Prozessor, Arbeitsspeicher>, Docker Desktop <Version>.
Postgres und Kafka im Compose, `batch-service` vom Host aus.

| Stufe | Nachrichten | Dauer | Nachrichten pro Sekunde |
|---|---|---|---|
| 1 — einzeln (`insertOne`, ein Commit pro Nachricht) | 100 000 | <Dauer> s | <Wert> |
| 2 — gebündelt (`insertBatch`, bis 500 pro Paket) | — | noch nicht gemessen | — |

Einstellungen: `max-poll-records: 500`, `fetch-max-wait: 200ms`, `fetch-min-size: 100KB`,
JDBC-URL mit `reWriteBatchedInserts=true`, Log-Level `INFO`.

Die Dauer zählt vom Start des Skripts bis zur letzten gespeicherten Nachricht; die Genauigkeit liegt
bei etwa ±2 Sekunden.
```

- [ ] **Schritt 5: Commit**

```bash
git add scripts/load-test.sh docs/messungen/
git commit -m "feat(scripts): Lastskript und Messung der Einzelstufe"
```

---

## Task 8: Stufe 2 — gebündelt speichern, mit Einzelweg und Messung

**Dateien:**
- Ersetzen: `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageListener.java`
- Ersetzen: `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageListenerTest.java`
- Ändern: `docs/messungen/<datum>-einzeln-vs-paket.md`

**Schnittstellen:**
- Braucht aus Task 2–7: dieselben Helfer wie Stufe 1, dazu `MessageWriter.insertBatch`.
- Liefert: `@KafkaListener`-Methode
  `void MessageListener.onMessages(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment)`.
  `onMessage` aus Stufe 1 fällt weg.

- [ ] **Schritt 1: Die Tests für Stufe 2 schreiben (ersetzen die Tests von Stufe 1)**

Datei `batch-service/src/test/java/ch/benedict/m321/batch/message/MessageListenerTest.java`
**vollständig ersetzen** durch:

```java
package ch.benedict.m321.batch.message;

import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Testet die Paketstufe des Listeners - je ein Test pro Fehlerklasse aus dem Design, dazu der
 * Einzelweg. Writer, Dead-Letter-Publisher und Acknowledgment sind Attrappen; der Parser ist
 * echt, damit kaputtes JSON wirklich als kaputt erkannt wird.
 */
class MessageListenerTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final MessageParser messageParser = new MessageParser(objectMapper);
    private final MessageWriter messageWriter = mock(MessageWriter.class);
    private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);

    private final MessageListener listener =
            new MessageListener(messageParser, messageWriter, deadLetterPublisher);

    /** Prueft den Normalfall: das ganze Paket mit einem Aufruf geschrieben, dann bestaetigt. */
    @Test
    void writesWholeBatchAndAcknowledges() {
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                recordWithValue(1, validJson(2)),
                recordWithValue(2, validJson(3)));

        listener.onMessages(records, acknowledgment);

        verify(messageWriter).insertBatch(argThat(batch -> batch.size() == 3));
        verify(messageWriter, never()).insertOne(any());
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /** Klasse 1: kaputtes JSON geht aufs Dead-Letter-Topic und kommt gar nicht erst ins INSERT. */
    @Test
    void brokenJsonIsLeftOutOfTheBatch() {
        ConsumerRecord<String, String> broken = recordWithValue(1, "{kaputt");
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                broken,
                recordWithValue(2, validJson(3)));

        listener.onMessages(records, acknowledgment);

        verify(deadLetterPublisher).publish(eq(broken), anyString());
        verify(messageWriter).insertBatch(argThat(batch -> batch.size() == 2));
        verify(acknowledgment).acknowledge();
    }

    /**
     * Klasse 2 im Paket: lehnt die Datenbank eine Zeile ab, wird das Paket Zeile fuer Zeile
     * geschrieben - nur die schuldige Zeile geht aufs Dead-Letter-Topic, das Paket wird bestaetigt.
     */
    @Test
    void rejectedRowFallsBackToOneByOne() {
        ConsumerRecord<String, String> second = recordWithValue(1, validJson(2));
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                second,
                recordWithValue(2, validJson(3)));
        doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .when(messageWriter).insertBatch(anyList());
        // Einzeln geschrieben scheitert nur die zweite Nachricht.
        doNothing()
                .doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .doNothing()
                .when(messageWriter).insertOne(any());

        listener.onMessages(records, acknowledgment);

        verify(messageWriter, times(3)).insertOne(any());
        verify(deadLetterPublisher).publish(eq(second), startsWith("Von der Datenbank abgelehnt"));
        verify(deadLetterPublisher, times(1)).publish(any(), anyString());
        verify(acknowledgment).acknowledge();
    }

    /**
     * Klasse 3: ist die Datenbank nicht erreichbar, fliegt der Fehler weiter und es wird NICHT
     * bestaetigt - Spring wiederholt das ganze Paket, nichts geht aufs Dead-Letter-Topic.
     */
    @Test
    void databaseDownIsPassedOnAndNotAcknowledged() {
        List<ConsumerRecord<String, String>> records = List.of(recordWithValue(0, validJson(1)));
        doThrow(new CannotGetJdbcConnectionException("Postgres weg"))
                .when(messageWriter).insertBatch(anyList());

        assertThrows(CannotGetJdbcConnectionException.class, () -> listener.onMessages(records, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    /**
     * Klasse 3 mitten im Einzelweg: faellt die Datenbank waehrend des Zeile-fuer-Zeile-Schreibens
     * aus, fliegt auch dieser Fehler weiter und das Paket bleibt unbestaetigt.
     */
    @Test
    void databaseDownDuringOneByOneIsPassedOn() {
        List<ConsumerRecord<String, String>> records = List.of(
                recordWithValue(0, validJson(1)),
                recordWithValue(1, validJson(2)));
        doThrow(new DataIntegrityViolationException("Fremdschluessel verletzt"))
                .when(messageWriter).insertBatch(anyList());
        doThrow(new CannotGetJdbcConnectionException("Postgres weg"))
                .when(messageWriter).insertOne(any());

        assertThrows(CannotGetJdbcConnectionException.class, () -> listener.onMessages(records, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    /** Baut gueltiges Nachrichten-JSON mit einer eigenen ID pro Nummer. */
    private String validJson(int number) {
        return String.format("{\"id\":\"aaaaaaaa-0000-0000-0000-%012d\","
                + "\"roomId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"sender\":\"lernende1\",\"text\":\"Hallo %d\","
                + "\"sentAt\":\"2026-09-11T08:05:00Z\"}", number, number);
    }

    /** Baut einen Topic-Eintrag, wie ihn Kafka dem Listener uebergibt. */
    private ConsumerRecord<String, String> recordWithValue(long offset, String value) {
        return new ConsumerRecord<>("chat.messages", 0, offset, "11111111-1111-1111-1111-111111111111", value);
    }
}
```

- [ ] **Schritt 2: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/batch-service
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `onMessages` gibt es noch nicht.

- [ ] **Schritt 3: Den Listener durch Stufe 2 ersetzen**

Datei `batch-service/src/main/java/ch/benedict/m321/batch/message/MessageListener.java`
**vollständig ersetzen** durch:

```java
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
        } catch (DataAccessException unreachable) {
            // Klasse 3: die Datenbank hat gerade ein Problem, zum Beispiel ist sie nicht erreichbar -
            // egal ob beim gebuendelten Schreiben oder mitten im Einzelweg. Ablehnungen einzelner
            // Zeilen (Klasse 2) kommen hier nie an, die faengt writeBatch selbst. Wir melden den
            // Ausfall laut und werfen den Fehler weiter: Spring wiederholt dann das ganze Paket,
            // bestaetigt wird nichts. Ohne diese Meldung liefe die Wiederholung still ab, und man
            // saehe den Ausfall nur am wachsenden Lag.
            Throwable databaseError = unreachable.getMostSpecificCause();
            String databaseMessage = databaseError.getMessage();
            log.error("Datenbank nicht erreichbar ({}) - Paket mit {} Nachrichten wird wiederholt",
                    databaseMessage, validMessages.size());
            throw unreachable;
        }
        acknowledgment.acknowledge();

        long elapsedMillis = System.currentTimeMillis() - startMillis;
        log.info("Paket mit {} Nachrichten in {} ms geschrieben", validMessages.size(), elapsedMillis);
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
```

- [ ] **Schritt 4: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 19, Failures: 0, Errors: 0` (14 aus Task 2–5, 5 neue
Listener-Tests; die 4 Tests der Stufe 1 sind ersetzt).

- [ ] **Schritt 5: Von Hand prüfen — ein gemischtes Paket**

`chat-service` und `batch-service` (jetzt Stufe 2) starten. Dann drei Einträge **auf einmal** auf das
Topic legen — einer gültig, einer kaputt, einer für einen unbekannten Raum:

```bash
printf '%s\t%s\n' \
  "11111111-1111-1111-1111-111111111111" '{"id":"bbbbbbbb-0000-4000-8000-000000000001","roomId":"11111111-1111-1111-1111-111111111111","sender":"lernende1","text":"Gut im Paket","sentAt":"2026-09-11T09:00:00Z"}' \
  "11111111-1111-1111-1111-111111111111" '{kaputt' \
  "99999999-9999-9999-9999-999999999999" '{"id":"bbbbbbbb-0000-4000-8000-000000000002","roomId":"99999999-9999-9999-9999-999999999999","sender":"lernende1","text":"Falscher Raum im Paket","sentAt":"2026-09-11T09:00:01Z"}' \
  | docker exec -i m321-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic chat.messages --property parse.key=true --property "key.separator=$(printf '\t')"
```

Erwartet im Log des `batch-service`:
- eine `WARN`-Zeile für das kaputte JSON (Klasse 1),
- eine `WARN`-Zeile «Paket mit 2 Nachrichten abgelehnt … – schreibe Zeile fuer Zeile» (Klasse 2)
  und danach eine `WARN`-Zeile für den unbekannten Raum,
- eine `INFO`-Zeile «Paket mit 2 Nachrichten in … ms geschrieben».

`GET /api/messages` für den Demo-Raum zeigt «Gut im Paket». Auf `chat.messages-dlt` (Befehl aus
Task 6, Schritt 6) stehen zwei neue Einträge mit ihrem Grund.

> Landen die drei Einträge in verschiedenen Paketen, weil Kafka sie getrennt ausliefert, fehlt
> die Zeile «Paket … abgelehnt». Dann den Befehl einfach wiederholen — oder die Einträge in einer
> einzigen Datei sammeln und mit `< datei` einspielen.

- [ ] **Schritt 6: Von Hand prüfen — Datenbank weg, nichts geht verloren**

```bash
docker stop m321-postgres
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -s -o /dev/null -w "%{http_code} " -X POST http://localhost:8080/api/messages \
    -H 'Content-Type: application/json' \
    -d "{\"roomId\":\"11111111-1111-1111-1111-111111111111\",\"sender\":\"lernende1\",\"text\":\"Waehrend Postgres weg ist $i\"}"
done
echo
```

Erwartet: zehnmal `202` — der `chat-service` braucht zum Senden keine Datenbank. Im Log des
`batch-service` alle 5 bis 10 Sekunden eine `ERROR`-Zeile «Datenbank nicht erreichbar (…) - Paket
mit … Nachrichten wird wiederholt»: 5 Sekunden pausiert die Fehlerbehandlung (`FixedBackOff`), dazu
wartet der Verbindungspool bis zu 5 Sekunden auf eine Verbindung (`connection-timeout`). Weist der
gestoppte Container die Verbindung sofort ab («Connection refused»), sind es eher 6 Sekunden. Nach
etwa 15 Sekunden:

```bash
docker exec m321-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group batch-service
```

Erwartet: in der Spalte `LAG` stehen zusammen 10. Dann:

```bash
docker start m321-postgres
```

Innerhalb von etwa 15 Sekunden: der Lag geht auf 0, im Log erscheint «Paket mit 10 Nachrichten …
geschrieben», und `GET` mit `limit=10` zeigt alle zehn Nachrichten. Auf `chat.messages-dlt` ist
**nichts** Neues dazugekommen — «gerade nicht» ist kein «nie».

- [ ] **Schritt 7: Stufe 2 messen**

Nur der `batch-service` muss laufen (der `chat-service` darf weiterlaufen). Dann mit derselben Zahl
wie in Task 7:

```bash
cd it3b-m321
scripts/load-test.sh 100000
```

- [ ] **Schritt 8: Die Messdatei ergänzen**

In `docs/messungen/<datum>-einzeln-vs-paket.md` die Zeile für Stufe 2 mit den echten Werten
füllen und darunter einen Abschnitt anhängen:

```markdown
## Einordnung

<Zwei bis vier Sätze auf Deutsch, aus den echten Zahlen: Faktor zwischen den Stufen; ob die
Datenbank oder das Lastskript bzw. Kafka der Engpass war (Hinweis: dauert der Lauf in Stufe 2
kaum länger als das Schreiben aufs Topic, war das Einspielen der Engpass, nicht der
batch-service); was die Zahl für die 100 000 Nachrichten pro Sekunde aus PLANUNG.md 2.3 bedeutet.>
```

- [ ] **Schritt 9: Commit**

```bash
git add batch-service/ docs/messungen/
git commit -m "feat(batch-service): Stufe 2 - gebuendelt speichern mit Einzelweg, Messung"
```

---

## Task 9: `PLANUNG.md` und `README.md` nachziehen

**Dateien:**
- Ändern: `PLANUNG.md`
- Ändern: `README.md`

**Schnittstellen:**
- Braucht aus Task 7 und 8: die gemessenen Werte in `docs/messungen/<datum>-einzeln-vs-paket.md`.

- [ ] **Schritt 1: Abschnitt 2.2, Schritt 3 ersetzen**

In `PLANUNG.md` Punkt 3 der Liste in Abschnitt 2.2 (beginnt mit «`chat-service` prüft das Token,
vergibt eine UUID …», endet mit «**Keine Datenbank im Anfrageweg.**») ersetzen durch:

```markdown
3. `chat-service` prüft das Token und mit **einer lesenden** Abfrage auf `room_member`, ob der
   Absender Mitglied des Raums ist (gebaut in Teilprojekt 2). Dann vergibt es eine UUID und einen
   Zeitstempel und schreibt die Nachricht mit `roomId` als **Schlüssel** auf das Topic
   `chat.messages`. Danach antwortet es dem Client. **Kein schreibender Datenbankzugriff im
   Anfrageweg** — gespeichert wird ausschliesslich im `batch-service`.
```

- [ ] **Schritt 2: Abschnitt 2.4 korrigieren**

In der Tabelle der drei Fälle:

a) An die Zelle «Was wir tun» der Zeile **Dauerhafter Rückstau** diesen Satz anhängen:

```markdown
Der `batch-service` selbst wiederholt das Paket bei nicht erreichbarer Datenbank alle 5 Sekunden ohne Ende und bestätigt nichts — der Lag wächst sichtbar, auf seiner Seite geht nichts verloren.
```

b) In der Zeile **Giftnachricht** die Zelle «Was wir tun» ersetzen durch:

```markdown
Der `batch-service` legt sie auf das Topic `chat.messages-dlt` (Grund im Header `error-reason`) und bestätigt. Kaputtes JSON geht sofort dorthin, eine von der Datenbank abgelehnte Zeile nach dem Einzelweg (siehe unten). Keine Wiederholungsschleife: diese Nachricht wird nie speicherbar.
```

c) Den Absatz «**Ein Haken, der zum Bündeln gehört.** …» (endet mit «Steht als offener Punkt drin.»)
ersetzen durch:

```markdown
**Ein Haken, der zum Bündeln gehört.** Lehnt die Datenbank eine einzige Zeile ab, scheitert das
ganze `batchUpdate` — alle 500. Antwort darauf: der `batch-service` schreibt dieses eine Paket dann
**einmalig Zeile für Zeile**. Nur die wirklich schuldige Zeile geht auf das Dead-Letter-Topic, die
restlichen 499 sind gespeichert; schon geschriebene Zeilen überspringt `ON CONFLICT`. Spring Kafkas
`@RetryableTopic` wäre hier keine Lösung: es wird für Batch-Listener gar nicht unterstützt (siehe
Nachtrag «batch-service»).
```

- [ ] **Schritt 3: Offene Punkte 3 und 5 ersetzen**

Punkt 3 ersetzen durch (Werte aus der Messdatei):

```markdown
3. **Paketgrösse und Wartezeit** — gemessen am <datum> (`docs/messungen/<datum>-einzeln-vs-paket.md`):
   einzeln <Wert> Nachrichten/s, gebündelt <Wert> Nachrichten/s, mit `max.poll.records: 500`,
   `fetch.max.wait.ms: 200`, `fetch.min.bytes: 100 KB`. Erneut nachmessen, sobald die Dienste im
   Compose laufen (Teilprojekt 4).
```

Punkt 5 ersetzen durch:

```markdown
5. **Einzelweg nach einem fehlgeschlagenen Paket** — erledigt im `batch-service` (Teilprojekt 1,
   Abschnitt 2.4).
```

- [ ] **Schritt 4: Abschnitt 5 «Nächste Schritte» ersetzen**

Den ganzen Abschnitt `## 5. Nächste Schritte` (bis vor die Trennlinie `---`) ersetzen durch:

```markdown
## 5. Nächste Schritte

Umgesetzt: Infrastruktur (Postgres, Kafka mit Volume), `chat-service` mit Lesen und Senden
(`docs/plan/2026-09-04-chat-service-bootstrap.md`), `batch-service` mit Einzel- und Paketstufe
(`docs/plan/2026-09-11-batch-service.md`).

Weiter in dieser Reihenfolge (entschieden am 2026-09-11), jedes Teilprojekt mit eigenem Design,
Plan und Umsetzung:

1. **Räume und Mitgliedsprüfung** — die Tabellen `room` und `room_member` gibt es schon; dazu die
   drei Endpunkte und die lesende Mitgliedsprüfung beim Senden und Lesen.
2. **Live-Anzeige per SSE** — `GET /stream`, jede `chat-service`-Instanz mit eigener, flüchtiger
   Consumer-Gruppe (`auto.offset.reset: latest`).
3. **Gateway und React-Oberfläche** — nginx als einziger offener Port; `chat-service` und
   `batch-service` wandern ins Compose, die veröffentlichten Ports werden zu `expose`.
4. **Keycloak-Login** — Realm-Import, Token-Prüfung, Absender aus dem Token statt aus dem Request.
5. **Zeitgesteuerte Aufgaben** im `batch-service` (Archivierung, Statistik) und **JavaFX-Client**.
```

- [ ] **Schritt 5: Nachtrag im «Verlauf» anhängen**

Ans Ende von `PLANUNG.md` anhängen (Werte aus der Messdatei):

```markdown
### Nachtrag — batch-service (Teilprojekt 1)

Der `batch-service` ist gebaut, nach dem Design
`docs/superpowers/specs/2026-09-11-batch-service-design.md` und dem Plan
`docs/plan/2026-09-11-batch-service.md`. Drei Punkte dieses Dokuments haben sich dabei geändert:

- **Die Raumprüfung beim Senden ist erlaubt.** Abschnitt 2.2 verbot «Datenbank im Anfrageweg»,
  Abschnitt 3 verlangte eine Abfrage auf `room_member` beim Senden — ein Widerspruch. Entschieden:
  gemeint war nur **schreibender** Zugriff. Eine lesende, per Index schnelle Abfrage ist erlaubt und
  verhindert, dass jemand in fremde oder nicht existierende Räume schreibt. Gebaut wird sie in
  Teilprojekt 2; bis dahin fängt der `batch-service` Nachrichten für unbekannte Räume über das
  Dead-Letter-Topic ab — und behält das auch danach als Sicherheitsnetz.
- **`@RetryableTopic` war falsch.** Spring Kafka unterstützt die nicht-blockierende Wiederholung
  nicht für Batch-Listener. Ersetzt durch eigenen, lesbaren Code: drei Fehlerklassen (kaputte
  Nachricht, abgelehnte Zeile, Infrastruktur weg), Einzelweg nach einem abgelehnten Paket, endlose
  Wiederholung alle 5 Sekunden bei nicht erreichbarer Datenbank.
- **Neue Reihenfolge der Teilprojekte.** Die React-Oberfläche kommt vor Keycloak, damit früher etwas
  Klickbares da ist; Keycloak ersetzt danach das vorläufige Absenderfeld.

Gemessen (`docs/messungen/<datum>-einzeln-vs-paket.md`): einzeln <Wert> Nachrichten/s, gebündelt
<Wert> Nachrichten/s.

**Was ich ohne Rückfrage festgelegt habe:** `spring-boot-starter-json` als zusätzliche Abhängigkeit
(ohne Web-Starter fehlt sonst der `ObjectMapper`), ein Volume für Kafka und
`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` im Compose, 5 Sekunden Wartezeit zwischen zwei Versuchen,
keine ausdrückliche Transaktion um ein Paket (`ON CONFLICT` macht das Wiederholen ungefährlich).
```

- [ ] **Schritt 6: `README.md` ergänzen**

a) In der Liste unter `## Dokumente` nach dem Eintrag zum Bootstrap-Plan einfügen:

```markdown
- [`docs/superpowers/specs/2026-09-11-batch-service-design.md`](docs/superpowers/specs/2026-09-11-batch-service-design.md)
  — Design des `batch-service`: Einzel- und Paketstufe, Fehlerklassen, Dead-Letter-Topic.
- [`docs/plan/2026-09-11-batch-service.md`](docs/plan/2026-09-11-batch-service.md)
  — Schritt-für-Schritt-Plan für den `batch-service`, mit Messung einzeln gegen gebündelt.
```

b) Den Abschnitt `## Stand` ersetzen durch:

```markdown
## Stand

Gebaut sind der `chat-service` (Verlauf lesen, Nachrichten senden) und der `batch-service`
(speichert die Nachrichten gebündelt). Eine Chat-Oberfläche gibt es noch nicht — ausprobieren
über die Swagger-Oberfläche des `chat-service`. Wie es weitergeht, steht in `PLANUNG.md`,
Abschnitt 5.

## Ausprobieren

1. Docker Desktop starten, dann im Projektwurzelverzeichnis `docker compose up -d`.
2. Im Ordner `chat-service` und danach im Ordner `batch-service` je `mvn spring-boot:run`
   (mit Java 21) — der `chat-service` zuerst, er legt das Kafka-Topic an.
3. <http://localhost:8080/swagger-ui.html> öffnen, mit `POST /api/messages` senden und mit
   `GET /api/messages` (Raum `11111111-1111-1111-1111-111111111111`) den Verlauf lesen.
```

- [ ] **Schritt 7: Prüfen, dass nichts Veraltetes stehen bleibt**

```bash
cd it3b-m321
grep -n "Keine Datenbank im Anfrageweg\|Steht als offener Punkt drin\|Noch nicht gebaut\." PLANUNG.md
```

Erwartet: nur noch Treffer, die zu **anderen** offenen Punkten gehören (Punkt 4, Lag-Schwelle) —
keiner zu Abschnitt 2.2, zum Einzelweg oder zum Haken beim Bündeln.

- [ ] **Schritt 8: Commit**

```bash
git add PLANUNG.md README.md
git commit -m "docs: PLANUNG.md und README nach dem batch-service nachziehen"
```

---

## Fertig, wenn …

- [ ] `mvn test` im Verzeichnis `batch-service` ist grün (19 Tests), `mvn test` im `chat-service` weiterhin
- [ ] `docker compose down && docker compose up -d` behält Topics und Offsets (Volume `kafka-data`)
- [ ] Eine per `POST /api/messages` gesendete Nachricht steht nach etwa einer Sekunde im Verlauf
- [ ] Unbekannter Raum und kaputtes JSON landen auf `chat.messages-dlt`, mit Grund im Header
- [ ] Mit gestopptem Postgres wächst der Lag; nach dem Start ist alles gespeichert, nichts im Dead-Letter-Topic
- [ ] `docs/messungen/<datum>-einzeln-vs-paket.md` enthält beide gemessenen Stufen und eine Einordnung
- [ ] `PLANUNG.md` enthält keinen Widerspruch mehr zwischen 2.2 und 3 und kein `@RetryableTopic` als Lösung
- [ ] Neun Commits liegen vor, einer pro Task

---

## Übungsaufgabe für die Klasse

Wird der Code an die Lernenden gegeben, **vor dem Austeilen** in `message/MessageListener.java` den
Rumpf von `rejectionReason(...)` entfernen und ersetzen durch:

```java
    /** Macht aus der Ablehnung der Datenbank einen lesbaren Grund fuer den Dead-Letter-Header. */
    private String rejectionReason(DataIntegrityViolationException rejected) {
        // TODO Übung: Die genaueste Ursache der Ausnahme holen und daraus den Grund
        //             "Von der Datenbank abgelehnt: <Meldung der Ursache>" zurueckgeben.
    }
```

Umfang: **zwei bis drei Zeilen.** Gebraucht werden `getMostSpecificCause()` der Ausnahme und
`getMessage()` der Ursache.

Diese Stelle ist mit Absicht gewählt: fehlt die Lösung, **kompiliert die Datei nicht** («missing
return statement»). Und der Lerninhalt passt zum Kapitel: Spring verpackt den Fehler von PostgreSQL
in eine eigene Ausnahme — wer den eigentlichen Grund will, muss ihn auspacken.

---

## Prüfungsstoff in diesem Plan

| Thema | Wo es im Code steht |
|---|---|
| **Consumer-Gruppe und Offsets** — Kafka merkt sich pro Gruppe, wie weit gelesen wurde; nach einem Neustart geht es dort weiter | `KafkaConfiguration.GROUP_ID`, Task 6 Schritt 7 |
| **Manuelles Bestätigen** — Offset erst speichern, wenn die Nachricht wirklich erledigt ist | `MessageListener` (`acknowledgment.acknowledge()`), `ack-mode: manual` |
| **At-least-once und Idempotenz** — Kafka liefert im Zweifel doppelt, `ON CONFLICT` macht das harmlos | `MessageWriter.INSERT_SQL`, Test `secondDeliveryOfTheSameBatchChangesNothing` |
| **Bündeln** — ein `batchUpdate` statt vieler `INSERT`, `reWriteBatchedInserts` | `MessageWriter.insertBatch`, `application.yml`, Messdatei |
| **Drei Fehlerklassen** — «nie» aufs Dead-Letter-Topic, «gerade nicht» endlos wiederholen | `MessageListener`, `KafkaConfiguration.endlessRetryErrorHandler` |
| **Dead-Letter-Topic** — sichtbare Ablage statt stiller Löcher | `DeadLetterPublisher` |
| **Backpressure sichtbar machen** — Consumer-Lag bei ausgefallener Datenbank | Task 8 Schritt 6 |
| **Gepruefte gegen ungepruefte Ausnahmen** — der Compiler erzwingt die Behandlung kaputter Nachrichten | `InvalidMessageException` |
| **Kein geteilter Code zwischen Diensten** — nur das JSON-Format ist gemeinsam | `ChatMessage` (eigene Kopie) |
| **Integrationstest mit Rollback** — gegen die echte Datenbank, ohne Spuren | `MessageWriterIntegrationTest` (`@JdbcTest`) |

---

## Entscheide in diesem Plan — und was daran diskutabel ist

| Entscheid | Begründung | Wenn es anders sein soll |
|---|---|---|
| **`spring-boot-starter-json` als Abhängigkeit** | Ohne Web-Starter bringt Spring Boot keinen `ObjectMapper` mit; der Parser braucht ihn. Der Starter liefert genau den `ObjectMapper`, den auch der `chat-service` benutzt. | Einen `ObjectMapper` von Hand bauen — dann aber mit denselben Einstellungen, sonst liest der Parser anders als der `chat-service` schreibt |
| **Keine ausdrückliche Transaktion um ein Paket** | `ON CONFLICT` macht jede Wiederholung ungefährlich; eine Transaktion brächte keine zusätzliche Sicherheit, aber ein Konzept mehr. | `TransactionTemplate` um `insertBatch` — dann ist ein Paket immer ganz oder gar nicht geschrieben |
| **Endlos wiederholen bei «Datenbank weg»** | Spring überspringt sonst nach wenigen Versuchen still — stiller Datenverlust. | Begrenzen und danach aufs Dead-Letter-Topic — dann aber mit einem eigenen Topic für «Infrastruktur», damit man es nicht mit echten Giftnachrichten verwechselt |
| **Zwei Listen mit gleichem Index im Paketweg** | Einfachste Form, die ohne neue Klasse auskommt; der Kommentar erklärt die Kopplung. | Ein kleiner `record ReceivedMessage(ConsumerRecord, ChatMessage)` — eine Klasse mehr, dafür keine parallelen Listen |
| **Gepruefte `InvalidMessageException`** | Der Compiler zwingt den Listener, kaputte Nachrichten zu behandeln. | `Optional<ChatMessage>` — kürzer, aber der Grund des Fehlers geht verloren |
| **Kafka legt keine Topics mehr selbst an** | Sonst entsteht `chat.messages` mit 1 statt 6 Partitionen, wenn der `batch-service` vor dem `chat-service` startet. | Das Topic zusätzlich im `batch-service` als `NewTopic` anmelden — dann aber an zwei Stellen dieselben Werte pflegen |
| **Log-Level `INFO`** | Eine Zeile pro Nachricht würde die Messung verfälschen. | Für die Fehlersuche `DEBUG` in `application.yml` |
| **Doppelte Einträge im Dead-Letter-Topic möglich** | Scheitert ein Paket an «Datenbank weg» *nachdem* eine kaputte Nachricht schon abgelegt ist, wird sie beim Wiederholen ein zweites Mal abgelegt. Im Chat-Verlauf entsteht nichts Doppeltes; das Dead-Letter-Topic ist nur eine Ablage zum Anschauen. | Die Einträge im Dead-Letter-Topic über ihre ID entdoppeln, wenn man sie einmal erneut einspielen will |
| **Integrationstest gegen die Compose-Datenbank statt Testcontainers** | Keine zusätzliche Bibliothek; `@JdbcTest` rollt jeden Test zurück. | Testcontainers — dann laufen die Tests auch ohne gestartetes Compose, braucht aber eine neue Abhängigkeit |

---

## Danach

Teilprojekt 2: **Räume und Mitgliedsprüfung** — eigenes Design, eigener Plan. Dort wird die lesende
Mitgliedsprüfung beim Senden und Lesen gebaut, die dieser Plan in `PLANUNG.md` festgehalten hat.
