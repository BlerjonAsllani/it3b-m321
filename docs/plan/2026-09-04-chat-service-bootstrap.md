# Chat-Service Bootstrap — Implementierungsplan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Ziel:** Ein lauffähiger `chat-service`, der über Swagger dokumentiert ist, den Nachrichtenverlauf
aus PostgreSQL liest und neue Nachrichten auf das Kafka-Topic `chat.messages` schreibt.

**Architektur:** Ein einzelner Spring-Boot-Dienst mit drei Schichten — Controller (HTTP und
Swagger-Doku), Service (Fachlogik) und Repository (SQL). Geschrieben wird in die
Nachrichtentabelle **nicht**: der `chat-service` publiziert nur, das Speichern übernimmt später der
`batch-service` (siehe `PLANUNG.md`, Abschnitt 2.3). PostgreSQL und Kafka laufen in
`docker-compose`, der Dienst selbst zunächst aus der IDE heraus.

**Tech-Stack:** Java 21 · Spring Boot 3.5.16 · springdoc-openapi 2.9.0 (Swagger UI) ·
Spring JDBC (`JdbcTemplate`) · Spring Kafka · PostgreSQL 17 · Apache Kafka 4 (KRaft) · Maven

**Spec:** `PLANUNG.md` (Abschnitte 1, 2.1, 2.2, 2.3, 3) und `docs/design/2026-08-28-chat-app-architektur.html`

---

## Globale Vorgaben

Diese Punkte gelten für **jede** Aufgabe in diesem Plan.

| Vorgabe | Wert |
|---|---|
| Java | **21** — nicht ein neueres System-JDK; `java -version` muss 21 zeigen |
| Spring Boot | **3.5.16** (`spring-boot-starter-parent`) |
| springdoc-openapi | **2.9.0** (`springdoc-openapi-starter-webmvc-ui`) — diese Version wird gegen Boot 3.5.16 gebaut |
| Basis-Paket | `ch.benedict.m321.chat` |
| **Sprache im Code** | **Englisch.** Klassen, Methoden, Variablen, Felder, Testmethoden, Tabellen- und Spaltennamen, JSON-Felder, Query-Parameter — alles englisch. |
| **Sprache in Text** | **Deutsch.** Kommentare, Javadoc, Log-Ausgaben, Fehlermeldungen an den Client und alle Swagger-Beschreibungen. |
| Codestil | `CLAUDE.md` im Projektwurzelverzeichnis: eine Anweisung pro Zeile, keine Stream-Ketten, keine Annotation-Magie, kurze Methoden |
| Kein Lombok | Das Projekt hat Lombok nicht als Abhängigkeit und bekommt sie auch nicht — `CLAUDE.md` verbietet Annotation-Magie |
| Logging | Jede Aktion loggt: Mutationen auf `INFO`, Repository-Schritte auf `DEBUG`, abgelehnte Anfragen auf `WARN` |
| Abhängigkeiten | Nur was im Plan steht. Keine zusätzliche Bibliothek ohne Rückfrage |

> **Warum englischer Code bei deutschen Kommentaren.** Java, Spring und SQL bringen ihr eigenes
> englisches Vokabular mit (`get`, `find`, `Repository`, `SELECT`). Mischt man deutsche Bezeichner
> darunter, entstehen Wortungetüme wie `findeLetzteNachrichtenByRaumId`. Bezeichner folgen also der
> Sprache der Werkzeuge, die Erklärung folgt der Sprache des Unterrichts.

### Was dieser Plan **nicht** enthält

Bewusst ausgelagert, damit der Bootstrap klein und prüfbar bleibt:

- **Keycloak und Token-Prüfung.** Der Absender kommt vorerst aus dem Request-Body. Sobald das
  Token da ist, wird das Feld ersatzlos gestrichen. Bis dahin steht in der Swagger-Doku
  ausdrücklich «Platzhalter bis Keycloak».
- **SSE (`GET /stream`).** Braucht einen `@KafkaListener` und eine eigene, flüchtige
  Consumer-Gruppe je Instanz — eigener Plan.
- **Raumverwaltung** (`POST /api/rooms`, Einladen, Mitgliedsprüfung). Die Tabellen werden hier schon
  angelegt, die Endpunkte kommen später.
- **`batch-service`, Gateway, React-App, JavaFX-Client.**

---

## Dateistruktur

```
it3b-m321/
├── .gitignore                                  neu
├── docker-compose.yml                          neu — PostgreSQL + Kafka
├── db/
│   ├── 01-schema.sql                           neu — room, room_member, message
│   └── 02-demo-data.sql                        neu — ein Raum + drei Nachrichten zum Ausprobieren
└── chat-service/
    ├── pom.xml                                 neu
    └── src/
        ├── main/
        │   ├── java/ch/benedict/m321/chat/
        │   │   ├── ChatServiceApplication.java          Startpunkt
        │   │   ├── OpenApiConfiguration.java            Titel/Beschreibung der Swagger-Doku
        │   │   ├── kafka/
        │   │   │   └── KafkaConfiguration.java          Topic-Name und Topic-Bean
        │   │   └── message/
        │   │       ├── Message.java                     Datensatz einer gespeicherten Nachricht
        │   │       ├── NewMessage.java                  Datensatz für den Request-Body
        │   │       ├── MessageRepository.java           SQL zum Lesen des Verlaufs
        │   │       ├── MessageService.java              Fachlogik: publizieren und lesen
        │   │       └── MessageController.java           REST-Endpunkte + Swagger-Annotationen
        │   └── resources/
        │       └── application.yml
        └── test/java/ch/benedict/m321/chat/
            ├── ChatServiceApplicationTest.java          Kontext startet
            └── message/MessageControllerTest.java       Endpunkte, ohne DB und ohne Broker
```

**Warum diese Aufteilung:** ein Paket pro Fachthema (`message`, `kafka`), nicht pro technischer
Schicht. Was zusammen geändert wird, liegt zusammen. Jede Datei hat genau eine Aufgabe, und keine
ist länger als etwa 80 Zeilen — so kann sie im Unterricht am Stück gelesen werden.

**Kein Eltern-POM, kein Multi-Modul-Maven.** Jeder Dienst bekommt später sein eigenes Verzeichnis
mit eigenem `pom.xml`. Das ist mehr Wiederholung, aber es zeigt genau das Modulthema: Dienste sind
unabhängig voneinander baubar und startbar.

---

## Task 1: Projekt anlegen — Spring Boot startet und Swagger UI ist erreichbar

**Dateien:**
- Erstellen: `.gitignore`
- Erstellen: `chat-service/pom.xml`
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/ChatServiceApplication.java`
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/OpenApiConfiguration.java`
- Erstellen: `chat-service/src/main/resources/application.yml`
- Test: `chat-service/src/test/java/ch/benedict/m321/chat/ChatServiceApplicationTest.java`

**Schnittstellen:**
- Liefert: die startfähige Anwendung auf Port `8080`, Swagger UI unter `/swagger-ui.html`,
  OpenAPI-JSON unter `/v3/api-docs`, Health-Endpunkt unter `/actuator/health`.

- [ ] **Schritt 1: Git-Repository anlegen**

Das Verzeichnis ist noch kein Git-Repository. Ohne Versionierung gibt es keine Commits und kein
Zurück.

```bash
cd it3b-m321
git init
```

Danach gleich die schon vorhandenen Dokumente sichern — Planung, Regeln und die Handskizze sind
bisher nirgends versioniert:

```bash
git add CLAUDE.md PLANUNG.md docs/
git commit -m "docs: Planung, Codestil-Regeln und Architekturskizze aufnehmen"
```

> `docs/design/mermaid.min.js` ist 3,5 MB gross. Die Datei liegt bewusst im Repo, damit das
> Architekturdokument auch ohne Internet rendert — auf Schul-Laptops ist das der Normalfall.

- [ ] **Schritt 2: `.gitignore` anlegen**

Datei `.gitignore` im Projektwurzelverzeichnis:

```gitignore
# Maven
target/

# IntelliJ
.idea/
*.iml
out/

# macOS
.DS_Store
```

- [ ] **Schritt 3: `chat-service/pom.xml` anlegen**

Nur Web, Swagger, Actuator und Test. Datenbank und Broker kommen in Task 2 dazu — so startet der
Dienst in diesem Schritt auch ohne laufendes Docker.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <!-- Der Spring-Boot-Eltern-POM legt die Versionen aller Spring-Bibliotheken fest.
         Deshalb steht bei den meisten Abhaengigkeiten unten keine Versionsnummer. -->
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.16</version>
        <relativePath/>
    </parent>

    <groupId>ch.benedict.m321</groupId>
    <artifactId>chat-service</artifactId>
    <version>0.1.0</version>
    <name>chat-service</name>
    <description>Nimmt Nachrichten entgegen und liest den Verlauf (Modul M321)</description>

    <properties>
        <java.version>21</java.version>
        <springdoc.version>2.9.0</springdoc.version>
    </properties>

    <dependencies>
        <!-- REST-Endpunkte und der eingebaute Webserver -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <!-- Swagger UI: erzeugt die API-Dokumentation aus den Annotationen im Controller -->
        <dependency>
            <groupId>org.springdoc</groupId>
            <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
            <version>${springdoc.version}</version>
        </dependency>

        <!-- Liefert /actuator/health. Damit sehen wir spaeter, ob DB und Broker erreichbar sind. -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
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

- [ ] **Schritt 4: `application.yml` anlegen**

Datei `chat-service/src/main/resources/application.yml`:

```yaml
server:
  port: 8080
  error:
    # Ohne diese Zeile schickt Spring Boot unsere deutschen Fehlermeldungen
    # (z. B. "text darf nicht leer sein") nicht an den Client, sondern nur den Statuscode.
    include-message: always

spring:
  application:
    name: chat-service

# Swagger UI liegt unter http://localhost:8080/swagger-ui.html
springdoc:
  swagger-ui:
    path: /swagger-ui.html
    # Endpunkte nach Reihenfolge im Controller sortieren statt alphabetisch
    operations-sorter: method

management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      show-details: always

logging:
  level:
    # Im Unterricht wollen wir jeden Schritt sehen.
    ch.benedict.m321: DEBUG
```

- [ ] **Schritt 5: Den fehlschlagenden Test schreiben**

Datei `chat-service/src/test/java/ch/benedict/m321/chat/ChatServiceApplicationTest.java`:

```java
package ch.benedict.m321.chat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Prueft, dass Spring alle Klassen zusammenbauen kann. Der Test hat absichtlich keinen
 * Rumpf: faellt beim Hochfahren irgendwo eine Bean weg oder ist eine Konfiguration
 * fehlerhaft, schlaegt er hier fehl, bevor irgendjemand die Anwendung startet.
 */
@SpringBootTest
class ChatServiceApplicationTest {

    @Test
    void contextLoads() {
        // Kein Inhalt noetig - der Test besteht darin, dass @SpringBootTest oben durchlaeuft.
    }
}
```

- [ ] **Schritt 6: Test laufen lassen — er muss fehlschlagen**

```bash
cd it3b-m321/chat-service
export JAVA_HOME=<Pfad-zu-deinem-JDK-21>   # nur nötig, wenn java -version nicht 21 zeigt
mvn test
```

Erwartet: **FEHLSCHLAG.** Meldung sinngemäss `Unable to find a @SpringBootConfiguration` — es gibt
noch keine Klasse mit `@SpringBootApplication`.

- [ ] **Schritt 7: Die Startklasse schreiben**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/ChatServiceApplication.java`:

```java
package ch.benedict.m321.chat;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Startpunkt des chat-service. Spring Boot faehrt von hier aus den eingebauten Webserver
 * hoch und durchsucht dieses Paket samt Unterpaketen nach Klassen, die es verwalten soll
 * (Controller, Service, Repository, Konfigurationen).
 */
@SpringBootApplication
public class ChatServiceApplication {

    /**
     * Uebergibt die Startklasse an Spring Boot. Alles Weitere - Webserver, Beans,
     * Konfigurationsdateien - erledigt der Aufruf darunter.
     */
    public static void main(String[] args) {
        SpringApplication.run(ChatServiceApplication.class, args);
    }
}
```

- [ ] **Schritt 8: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Schritt 9: Titel und Beschreibung der Swagger-Doku setzen**

Ohne diese Klasse heisst die Doku «OpenAPI definition» — nichtssagend. Datei
`chat-service/src/main/java/ch/benedict/m321/chat/OpenApiConfiguration.java`:

```java
package ch.benedict.m321.chat;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Setzt Titel, Version und Beschreibung der Swagger-Oberflaeche. Ohne diese Klasse
 * traegt die Dokumentation nur den Standardtitel "OpenAPI definition".
 */
@Configuration
public class OpenApiConfiguration {

    /**
     * Baut das Kopf-Objekt der API-Dokumentation. springdoc nimmt diese Bean und
     * ergaenzt sie um alles, was es in den Controllern findet.
     */
    @Bean
    public OpenAPI chatOpenApi() {
        Info info = new Info();
        info.setTitle("Chat-Service API");
        info.setVersion("0.1.0");
        info.setDescription(
                "REST-Schnittstelle des chat-service (Modul M321). "
                + "Neue Nachrichten werden entgegengenommen und an Kafka weitergegeben. "
                + "Der Verlauf wird aus PostgreSQL gelesen. "
                + "Der chat-service schreibt selbst NICHT in die Nachrichtentabelle.");

        OpenAPI documentation = new OpenAPI();
        documentation.setInfo(info);
        return documentation;
    }
}
```

- [ ] **Schritt 10: Anwendung starten und Swagger UI im Browser prüfen**

```bash
mvn spring-boot:run
```

Dann im Browser öffnen: <http://localhost:8080/swagger-ui.html>

Erwartet: die Seite lädt und zeigt oben **«Chat-Service API 0.1.0»** mit der Beschreibung.
Endpunkte sind noch keine da — das ist richtig so.

Zweite Prüfung, im Terminal:

```bash
curl -s http://localhost:8080/v3/api-docs | head -c 200
curl -s http://localhost:8080/actuator/health
```

Erwartet: JSON, das mit `{"openapi":"3.1.0","info":{"title":"Chat-Service API"` beginnt, und
`{"status":"UP",...}`.

Anwendung mit `Ctrl+C` beenden.

- [ ] **Schritt 11: Commit**

```bash
cd it3b-m321
git add .gitignore chat-service/
git commit -m "feat(chat-service): Projekt aufsetzen, Swagger UI erreichbar"
```

---

## Task 2: Infrastruktur — PostgreSQL und Kafka in docker-compose

**Dateien:**
- Erstellen: `docker-compose.yml`
- Erstellen: `db/01-schema.sql`
- Erstellen: `db/02-demo-data.sql`
- Ändern: `chat-service/pom.xml` (drei Abhängigkeiten ergänzen)
- Ändern: `chat-service/src/main/resources/application.yml` (Datenbank und Broker eintragen)

**Schnittstellen:**
- Braucht aus Task 1: `application.yml`, `pom.xml`
- Liefert: eine erreichbare Datenbank `chat` mit den Tabellen `room`, `room_member`, `message`
  samt Demo-Daten, und einen Kafka-Broker im KRaft-Modus.

> **Achtung, bewusste Abweichung von der Vorgabe.** `PLANUNG.md` verlangt, dass nur Port 8080 nach
> aussen offen ist. In diesem Bootstrap läuft der `chat-service` aber noch auf dem Host (aus der
> IDE), nicht im Compose — er muss die Datenbank und den Broker also über `localhost` erreichen.
> Deshalb sind `5432` und `9092` hier **veröffentlicht**. Sobald der `chat-service` selbst
> im Compose läuft, werden diese `ports:`-Einträge zu `expose:` und die Regel gilt wieder.
> Das ist im Plan festgehalten, damit es später nicht vergessen geht.

> **Kein ZooKeeper.** Ältere Kafka-Anleitungen im Netz starten immer zwei Container: Kafka **und**
> ZooKeeper. Seit Kafka 3.3 gibt es den **KRaft-Modus**, in dem Kafka seine Metadaten selbst
> verwaltet; ab Kafka 4 ist ZooKeeper ganz entfallen. Wir brauchen also genau einen Container.
> Findet ihr ein Tutorial mit ZooKeeper, ist es veraltet.

- [ ] **Schritt 1: `docker-compose.yml` anlegen**

Datei `docker-compose.yml` im Projektwurzelverzeichnis:

```yaml
# Infrastruktur fuer den Bootstrap: Datenbank und Message Broker.
# Der chat-service laeuft in dieser Phase noch auf dem Host (aus der IDE).
services:

  postgres:
    image: postgres:17-alpine
    container_name: m321-postgres
    environment:
      POSTGRES_DB: chat
      POSTGRES_USER: chat
      POSTGRES_PASSWORD: chat
    ports:
      # Nur auf localhost veroeffentlicht (127.0.0.1), nicht auf allen Netzwerk-
      # Schnittstellen: sonst waere die Datenbank (Zugang chat/chat) aus dem ganzen
      # Schulnetz erreichbar. Veroeffentlicht wird trotzdem, weil der chat-service in
      # diesem Bootstrap noch auf dem Host laeuft und die Datenbank so erreichen muss.
      - "127.0.0.1:5432:5432"
    volumes:
      # Alle .sql-Dateien hier drin fuehrt das Postgres-Image beim ERSTEN Start
      # in alphabetischer Reihenfolge aus. Danach nie wieder.
      - ./db:/docker-entrypoint-initdb.d:ro
      - postgres-data:/var/lib/postgresql/data
    networks:
      - chat-net

  kafka:
    image: apache/kafka:4.0.0
    container_name: m321-kafka
    ports:
      # Nur auf localhost veroeffentlicht, aus demselben Grund wie bei Postgres oben:
      # sonst waere der Broker aus dem ganzen Schulnetz erreichbar.
      - "127.0.0.1:9092:9092"
    environment:
      # --- KRaft: dieser eine Container ist Broker UND Controller zugleich ---
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # Worauf der Broker HOERT (im Container).
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

      # Welche Adresse der Broker den Clients NENNT: verbindet sich ein Client zu einer
      # falschen oder nicht aufloesbaren Adresse, haengt er danach im Timeout, obwohl die
      # erste Verbindung zum bootstrap-server geklappt hat - die haeufigste Fehlerquelle
      # bei Kafka in Docker.
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092

      # Ein einzelner Broker kann nichts replizieren - daher ueberall 1.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # Ohne das wartet die erste Consumer-Gruppe beim Start 3 Sekunden.
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    networks:
      - chat-net

volumes:
  postgres-data:

networks:
  chat-net:
    driver: bridge
```

- [ ] **Schritt 2: Schema anlegen**

Datei `db/01-schema.sql`:

```sql
-- Schema der Chat-App (Modul M321).
-- Wird vom Postgres-Image beim ERSTEN Start automatisch ausgefuehrt.

CREATE TABLE room (
    id         UUID         PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL
);

-- Wer darf in welchem Raum mitlesen. Der Schluessel besteht aus beiden Spalten,
-- damit dieselbe Person nicht zweimal im selben Raum stehen kann.
CREATE TABLE room_member (
    room_id    UUID         NOT NULL REFERENCES room (id),
    username   VARCHAR(100) NOT NULL,
    invited_by VARCHAR(100) NOT NULL,
    joined_at  TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (room_id, username)
);

CREATE TABLE message (
    -- Die ID kommt vom chat-service, NICHT von der Datenbank. Nur so kann der
    -- batch-service ein Paket gefahrlos wiederholen (ON CONFLICT DO NOTHING).
    id       UUID         PRIMARY KEY,
    room_id  UUID         NOT NULL REFERENCES room (id),
    sender   VARCHAR(100) NOT NULL,
    text     TEXT         NOT NULL,
    -- Zeitpunkt des SENDENS, gesetzt vom chat-service. Absichtlich kein DEFAULT now():
    -- sonst haetten alle 500 Nachrichten eines Pakets dieselbe Zeit.
    sent_at  TIMESTAMPTZ  NOT NULL
);

-- Der Verlauf wird immer pro Raum und nach Zeit sortiert gelesen.
-- Genau dafuer ist dieser Index da.
CREATE INDEX idx_message_room_time ON message (room_id, sent_at DESC);
```

- [ ] **Schritt 3: Demo-Daten anlegen**

Ohne Daten liefert `GET /api/messages` eine leere Liste und man sieht nicht, ob es funktioniert.

Datei `db/02-demo-data.sql`:

```sql
-- Ein Raum und drei Nachrichten zum Ausprobieren.
-- Die feste UUID des Raums steht auch im Plan und in der Swagger-Doku als Beispiel.

INSERT INTO room (id, name, created_by, created_at) VALUES
  ('11111111-1111-1111-1111-111111111111', 'Allgemein', 'lehrperson', now());

INSERT INTO room_member (room_id, username, invited_by, joined_at) VALUES
  ('11111111-1111-1111-1111-111111111111', 'lehrperson', 'lehrperson', now()),
  ('11111111-1111-1111-1111-111111111111', 'lernende1',  'lehrperson', now());

INSERT INTO message (id, room_id, sender, text, sent_at) VALUES
  ('aaaaaaaa-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111',
   'lehrperson', 'Willkommen im Raum Allgemein.',     now() - interval '3 minutes'),
  ('aaaaaaaa-0000-0000-0000-000000000002', '11111111-1111-1111-1111-111111111111',
   'lernende1',  'Danke, der Verlauf wird gelesen.',  now() - interval '2 minutes'),
  ('aaaaaaaa-0000-0000-0000-000000000003', '11111111-1111-1111-1111-111111111111',
   'lehrperson', 'Genau, geschrieben wird spaeter.',  now() - interval '1 minute');
```

- [ ] **Schritt 4: Compose starten und Schema prüfen**

```bash
cd it3b-m321
docker compose up -d
docker compose ps
docker exec -it m321-postgres psql -U chat -d chat -c "\dt"
docker exec -it m321-postgres psql -U chat -d chat -c "SELECT sender, text FROM message ORDER BY sent_at;"
```

Erwartet: beide Container laufen, `\dt` zeigt `message`, `room`, `room_member`, und die drei
Demo-Nachrichten in der richtigen Reihenfolge.

Dann prüfen, dass der Broker antwortet. Kafka bringt keine Weboberfläche mit — geprüft wird mit
den Kommandozeilenwerkzeugen, die im Container liegen:

```bash
docker exec -it m321-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Erwartet: der Befehl läuft durch und gibt eine **leere** Liste aus (oder nur interne Topics).
Wichtig ist nicht die Ausgabe, sondern dass er nicht in einen Timeout läuft — das wäre das
Zeichen, dass der Broker nicht erreichbar ist.

> **Die häufigste Kafka-Stolperfalle: `advertised.listeners`.** Ein Kafka-Client verbindet sich
> zuerst zum `bootstrap-server` und bekommt von dort die Adressen, unter denen die Broker
> **wirklich** erreichbar sind. Der Client spricht danach diese genannte Adresse an — nicht mehr
> die, die er selbst eingetippt hat. Steht in `KAFKA_ADVERTISED_LISTENERS` ein Hostname, den der
> Client nicht auflösen kann, verbindet sich der Client erfolgreich und hängt dann trotzdem im
> Timeout. Weil der `chat-service` in diesem Bootstrap auf dem **Host** läuft, steht dort
> `localhost:9092`. Wandert er später ins Compose, muss ein **zweiter** Listener mit dem
> Docker-internen Namen `kafka:9092` dazukommen — sonst findet der Dienst den Broker nicht.

> **Wenn das Schema später geändert wird:** die Skripte in `docker-entrypoint-initdb.d` laufen nur
> beim allerersten Start auf ein leeres Volume. Danach hilft nur
> `docker compose down -v && docker compose up -d`. Das löscht alle Daten — im Unterricht genau
> richtig, in Produktion nimmt man dafür Flyway.

- [ ] **Schritt 5: Abhängigkeiten in `pom.xml` ergänzen**

In `chat-service/pom.xml` **vor** `spring-boot-starter-test` einfügen:

```xml
        <!-- JdbcTemplate: SQL direkt, ohne ORM. Wir sehen jede Abfrage im Klartext. -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>

        <!-- Treiber fuer PostgreSQL. Wird nur zur Laufzeit gebraucht, nicht beim Kompilieren. -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Spring Kafka: die Anbindung an Kafka (Topic, Partition, Offset, send).
             Anders als bei den Starters oben steht hier die groupId
             org.springframework.kafka - die Version kommt trotzdem vom Eltern-POM. -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
```

- [ ] **Schritt 6: Datenbank und Broker in `application.yml` eintragen**

In `chat-service/src/main/resources/application.yml` den Block unter `spring:` erweitern:

```yaml
spring:
  application:
    name: chat-service

  # Verbindung zur Datenbank aus docker-compose.
  # Zugangsdaten stehen hier im Klartext, weil das eine Uebungsumgebung ist.
  datasource:
    url: jdbc:postgresql://localhost:5432/chat
    username: chat
    password: chat

  kafka:
    # Einstiegspunkt. Von hier holt sich der Client die echten Broker-Adressen
    # (advertised.listeners) - zeigen die auf einen falschen oder unerreichbaren Host,
    # verbindet sich der Client zwar hierhin, haengt danach aber im Timeout.
    bootstrap-servers: localhost:9092
    producer:
      # Schluessel und Wert gehen beide als Text raus: der Schluessel ist die roomId,
      # der Wert die Nachricht als JSON-Text. Das JSON bauen wir selbst im MessageService,
      # nicht mit Spring Kafkas JsonSerializer: der schreibt Zeitpunkte als Zahl und haengt
      # einen Header mit dem Klassennamen des Senders an, den andere Dienste nicht kennen.
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        # Wie lange send() hoechstens blockieren darf, wenn der Broker gar nicht
        # erreichbar ist. Standard waeren 60 Sekunden - so lange haengt sonst jede
        # Sendeanfrage. Die eckigen Klammern sorgen dafuer, dass Spring die Punkte
        # im Namen stehen laesst, statt sie als Verschachtelung zu lesen.
        "[max.block.ms]": 5000
```

- [ ] **Schritt 7: Test laufen lassen — der Kontext muss weiterhin starten**

```bash
cd chat-service
export JAVA_HOME=<Pfad-zu-deinem-JDK-21>   # nur nötig, wenn java -version nicht 21 zeigt
mvn test
```

Erwartet: **BESTANDEN.** Ab jetzt braucht dieser Test ein laufendes `docker compose up -d` — ohne
Datenbank kommt Spring beim Hochfahren nicht durch. Das ist gewollt und ehrlich: der Test prüft
genau das Zusammenspiel.

- [ ] **Schritt 8: Health-Endpunkt prüfen**

```bash
mvn spring-boot:run
```

In einem zweiten Terminal:

```bash
curl -s http://localhost:8080/actuator/health
```

Erwartet: `"status":"UP"` und darin der Eintrag `"db"` mit `"status":"UP"`. Das ist der Beweis,
dass die Datenbankverbindung wirklich steht — nicht nur, dass die Anwendung gestartet ist.

> **Kafka steht hier bewusst nicht.** Spring Boot liefert fertige Health-Indikatoren für die
> Datenbank, für RabbitMQ, für Redis und einige andere — für Kafka aber **keinen**. Es ist also
> kein Fehler, wenn in `/actuator/health` kein Kafka-Eintrag auftaucht; den gibt es schlicht
> nicht. Dass der Broker erreichbar ist, haben wir in Task 2 mit `kafka-topics.sh --list`
> geprüft, und in Task 4 sehen wir die Nachricht im Topic ankommen.

Anwendung mit `Ctrl+C` beenden.

- [ ] **Schritt 9: Commit**

```bash
cd it3b-m321
git add docker-compose.yml db/ chat-service/
git commit -m "feat(infra): PostgreSQL und Kafka in docker-compose, Schema und Demo-Daten"
```

---

## Task 3: Verlauf lesen — `GET /api/messages`

**Dateien:**
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/message/Message.java`
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageRepository.java`
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageService.java`
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageController.java`
- Test: `chat-service/src/test/java/ch/benedict/m321/chat/message/MessageControllerTest.java`

**Schnittstellen:**
- Braucht aus Task 2: Tabelle `message`, Demo-Daten, `JdbcTemplate` aus dem Starter.
- Liefert für Task 4:
  - `record Message(UUID id, UUID roomId, String sender, String text, Instant sentAt)`
  - `MessageService` als Spring-Bean mit
    `List<Message> loadHistory(UUID roomId, int limit)`
  - `MessageController` mit dem Pfadpräfix `/api/messages`

- [ ] **Schritt 1: Den fehlschlagenden Test schreiben**

Der Test lädt **nur** die Webschicht und ersetzt den Service durch eine Attrappe. Er braucht damit
weder Datenbank noch Broker und läuft überall.

Datei `chat-service/src/test/java/ch/benedict/m321/chat/message/MessageControllerTest.java`:

```java
package ch.benedict.m321.chat.message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Testet den Controller allein. @WebMvcTest startet nur die Webschicht, nicht die
 * ganze Anwendung - deshalb braucht dieser Test weder Datenbank noch Kafka.
 * Der Service wird durch eine Attrappe (@MockitoBean) ersetzt, die wir steuern.
 */
@WebMvcTest(MessageController.class)
class MessageControllerTest {

    private static final UUID ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MessageService messageService;

    @Test
    void historyReturnsMessagesAsJson() throws Exception {
        // Die Attrappe soll genau eine Nachricht zurueckgeben.
        Message example = new Message(
                UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"),
                ROOM_ID,
                "lehrperson",
                "Willkommen im Raum Allgemein.",
                Instant.parse("2026-09-04T08:00:00Z"));
        when(messageService.loadHistory(any(), anyInt())).thenReturn(List.of(example));

        mockMvc.perform(get("/api/messages").param("roomId", ROOM_ID.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].sender").value("lehrperson"))
                .andExpect(jsonPath("$[0].text").value("Willkommen im Raum Allgemein."));
    }
}
```

- [ ] **Schritt 2: Test laufen lassen — er muss fehlschlagen**

```bash
cd chat-service
export JAVA_HOME=<Pfad-zu-deinem-JDK-21>   # nur nötig, wenn java -version nicht 21 zeigt
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `MessageController`, `MessageService` und
`Message` gibt es noch nicht.

- [ ] **Schritt 3: Den Datensatz `Message` schreiben**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/message/Message.java`:

```java
package ch.benedict.m321.chat.message;

import java.time.Instant;
import java.util.UUID;

/**
 * Eine Nachricht, so wie sie in der Datenbank steht und wie sie ueber die API
 * herausgeht. Ein "record" ist eine Kurzform fuer eine Klasse, die nur Daten haelt:
 * Java erzeugt Konstruktor, Lesemethoden, equals und toString selbst.
 * Die Felder sind unveraenderlich - einmal gesetzt, bleibt eine Nachricht, wie sie ist.
 */
public record Message(
        UUID id,
        UUID roomId,
        String sender,
        String text,
        Instant sentAt) {
}
```

- [ ] **Schritt 4: Das Repository schreiben**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageRepository.java`:

```java
package ch.benedict.m321.chat.message;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Liest Nachrichten aus der Datenbank. Bewusst ohne ORM: das SQL steht im Klartext
 * da und jede Spalte wird von Hand in ein Feld uebertragen - man kann es Zeile fuer
 * Zeile vorlesen.
 *
 * Schreiben gibt es hier absichtlich nicht. In die Nachrichtentabelle schreibt
 * ausschliesslich der batch-service (siehe PLANUNG.md, Abschnitt 2.3).
 */
@Repository
public class MessageRepository {

    private static final Logger log = LoggerFactory.getLogger(MessageRepository.class);

    private final JdbcTemplate jdbcTemplate;

    /**
     * Spring reicht den JdbcTemplate hier herein (Konstruktor-Injektion). Wir bauen
     * ihn nicht selbst - dann koennte man ihn im Test nicht austauschen.
     */
    public MessageRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Holt die letzten Nachrichten eines Raums, neueste zuerst.
     * Der Index aus 01-schema.sql passt genau auf diese Abfrage.
     */
    public List<Message> findLatest(UUID roomId, int limit) {
        String sql = "SELECT id, room_id, sender, text, sent_at "
                   + "FROM message "
                   + "WHERE room_id = ? "
                   + "ORDER BY sent_at DESC "
                   + "LIMIT ?";

        log.debug("Lese die letzten {} Nachrichten aus Raum {}", limit, roomId);

        // Die Fragezeichen werden vom Treiber gefuellt. Niemals Werte in den
        // SQL-String kleben - das waere eine Einladung fuer SQL-Injection.
        List<Message> found = jdbcTemplate.query(sql, this::mapRow, roomId, limit);

        log.debug("{} Nachrichten aus Raum {} gelesen", found.size(), roomId);
        return found;
    }

    /**
     * Uebertraegt eine Ergebniszeile der Datenbank in ein Message-Objekt.
     * Diese Methode wird oben pro gefundener Zeile einmal aufgerufen.
     */
    private Message mapRow(ResultSet row, int rowNumber) throws SQLException {
        UUID id = row.getObject("id", UUID.class);
        UUID roomId = row.getObject("room_id", UUID.class);
        String sender = row.getString("sender");
        String text = row.getString("text");
        Timestamp timestamp = row.getTimestamp("sent_at");
        Instant sentAt = timestamp.toInstant();
        return new Message(id, roomId, sender, text, sentAt);
    }
}
```

- [ ] **Schritt 5: Den Service schreiben (vorerst nur Lesen)**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageService.java`:

```java
package ch.benedict.m321.chat.message;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Fachlogik rund um Nachrichten. Der Controller kennt nur diese Klasse, nicht das
 * Repository und nicht Kafka - so bleibt die Weboberflaeche von der Technik
 * dahinter getrennt.
 */
@Service
public class MessageService {

    private static final Logger log = LoggerFactory.getLogger(MessageService.class);

    private final MessageRepository messageRepository;

    public MessageService(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    /**
     * Liefert den Verlauf eines Raums. Reines Lesen - hier wird nichts veraendert.
     */
    public List<Message> loadHistory(UUID roomId, int limit) {
        log.debug("Verlauf angefordert: Raum {}, hoechstens {} Nachrichten", roomId, limit);
        return messageRepository.findLatest(roomId, limit);
    }
}
```

- [ ] **Schritt 6: Den Controller schreiben**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageController.java`:

```java
package ch.benedict.m321.chat.message;

import java.util.List;
import java.util.UUID;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Die REST-Schnittstelle fuer Nachrichten. Die Annotationen aus io.swagger.v3
 * beschreiben jeden Endpunkt - daraus baut springdoc die Swagger-Oberflaeche.
 * Was hier nicht beschrieben ist, taucht in der Dokumentation auch nicht auf.
 */
@RestController
@RequestMapping("/api/messages")
@Tag(name = "Nachrichten", description = "Nachrichten senden und den Verlauf eines Raums lesen")
public class MessageController {

    private static final Logger log = LoggerFactory.getLogger(MessageController.class);

    /** Obergrenze fuer "limit". Schuetzt die Datenbank vor einer Abfrage ueber Millionen Zeilen. */
    private static final int MAX_LIMIT = 100;

    private final MessageService messageService;

    public MessageController(MessageService messageService) {
        this.messageService = messageService;
    }

    /**
     * Liefert die letzten Nachrichten eines Raums, neueste zuerst.
     * Gelesen wird direkt aus der Datenbank - dieser Weg laeuft voellig getrennt
     * vom Senden ueber Kafka.
     */
    @Operation(
            summary = "Verlauf eines Raums lesen",
            description = "Gibt die letzten Nachrichten eines Raums zurueck, neueste zuerst. "
                        + "Demo-Raum zum Ausprobieren: 11111111-1111-1111-1111-111111111111")
    @ApiResponse(responseCode = "200", description = "Verlauf, moeglicherweise leer")
    @ApiResponse(responseCode = "400", description = "limit ist kleiner als 1 oder groesser als 100")
    @GetMapping
    public List<Message> history(
            @Parameter(description = "ID des Raums", required = true,
                       example = "11111111-1111-1111-1111-111111111111")
            @RequestParam UUID roomId,

            @Parameter(description = "Wie viele Nachrichten hoechstens (1 bis 100)", example = "50")
            @RequestParam(defaultValue = "50") int limit) {

        log.info("Verlauf abgerufen: Raum {}, limit {}", roomId, limit);

        // Grenzen pruefen, bevor die Zahl in die SQL-Abfrage geht.
        if (limit < 1 || limit > MAX_LIMIT) {
            log.warn("Ungueltiges limit {} fuer Raum {} - Anfrage abgelehnt", limit, roomId);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "limit muss zwischen 1 und " + MAX_LIMIT + " liegen");
        }

        List<Message> history = messageService.loadHistory(roomId, limit);
        log.info("Verlauf geliefert: Raum {}, {} Nachrichten", roomId, history.size());
        return history;
    }
}
```

- [ ] **Schritt 7: Test laufen lassen — er muss bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 2, Failures: 0, Errors: 0` (der Kontext-Test aus Task 1 und
der neue Controller-Test).

> **Docker muss laufen.** Seit Task 2 fährt `ChatServiceApplicationTest` die ganze
> Anwendung hoch und braucht dafür Datenbank und Broker. Vorher im
> Projektwurzelverzeichnis `docker compose up -d` ausführen.

- [ ] **Schritt 8: Von Hand in Swagger UI prüfen**

```bash
docker compose up -d      # falls noch nicht laufend, vom Projektwurzelverzeichnis aus
cd chat-service && mvn spring-boot:run
```

<http://localhost:8080/swagger-ui.html> öffnen. Erwartet:
- Es gibt jetzt den Bereich **«Nachrichten»** mit einem Endpunkt `GET /api/messages`.
- «Try it out» → `roomId` = `11111111-1111-1111-1111-111111111111` → «Execute».
- Antwort `200` mit den **drei** Demo-Nachrichten, neueste zuerst.
- Zweiter Versuch mit `limit` = `0` → Antwort `400`.

In der Konsole müssen dabei die Log-Zeilen `Verlauf abgerufen`, `Lese die letzten …` und
`Verlauf geliefert` erscheinen. Das ist der Beweis, dass alle drei Schichten durchlaufen wurden.

- [ ] **Schritt 9: Commit**

```bash
cd it3b-m321
git add chat-service/
git commit -m "feat(chat-service): GET /api/messages liest den Verlauf, dokumentiert in Swagger"
```

---

## Task 4: Nachricht senden — `POST /api/messages` schreibt auf das Topic

**Dateien:**
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/kafka/KafkaConfiguration.java`
- Erstellen: `chat-service/src/main/java/ch/benedict/m321/chat/message/NewMessage.java`
- Ändern: `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageService.java`
- Ändern: `chat-service/src/main/java/ch/benedict/m321/chat/message/MessageController.java`
- Test: `chat-service/src/test/java/ch/benedict/m321/chat/message/MessageControllerTest.java` (ergänzen)
- Test: `chat-service/src/test/java/ch/benedict/m321/chat/message/MessageServiceTest.java` (neu)

**Schnittstellen:**
- Braucht aus Task 3: `Message`, `MessageService`, `MessageController`
- Liefert für spätere Pläne:
  - `KafkaConfiguration.TOPIC_NAME` = `"chat.messages"`
  - `record NewMessage(UUID roomId, String sender, String text)`
  - `Message MessageService.sendMessage(NewMessage incoming)`

- [ ] **Schritt 1: Die fehlschlagenden Tests schreiben**

In `MessageControllerTest` **ergänzen**. Zuerst diese beiden Importe zu den bestehenden dazunehmen
(als echte `import`-Zeilen, nicht als Kommentar):

```java
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.server.ResponseStatusException;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
```

`any` ist bereits importiert. Dann die vier Testmethoden in die Klasse einfügen:

```java
    @Test
    void sendAcceptsMessageAndReturns202() throws Exception {
        Message created = new Message(
                UUID.fromString("bbbbbbbb-0000-0000-0000-000000000001"),
                ROOM_ID,
                "lernende1",
                "Hallo zusammen",
                Instant.parse("2026-09-04T08:05:00Z"));
        when(messageService.sendMessage(any())).thenReturn(created);

        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "Hallo zusammen"
                }
                """;

        // 202 Accepted heisst: angenommen und weitergegeben - aber noch nicht gespeichert.
        // Genau das ist bei uns der Fall, denn schreiben wird spaeter der batch-service.
        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value("bbbbbbbb-0000-0000-0000-000000000001"))
                .andExpect(jsonPath("$.text").value("Hallo zusammen"));
    }

    @Test
    void sendRejectsBlankText() throws Exception {
        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "   "
                }
                """;

        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());

        // Bei einer abgelehnten Anfrage darf der Service gar nicht erst aufgerufen werden.
        verify(messageService, never()).sendMessage(any());
    }

    @Test
    void sendRejectsTooLongText() throws Exception {
        // 2001 Zeichen - einer mehr als MAX_TEXT_LENGTH im Controller erlaubt.
        String zuLangerText = "a".repeat(2001);
        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "%s"
                }
                """.formatted(zuLangerText);

        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());

        verify(messageService, never()).sendMessage(any());
    }

    @Test
    void sendReturns503WhenKafkaDoesNotConfirm() throws Exception {
        // Die Attrappe verhaelt sich so wie der echte Service, wenn Kafka die
        // Nachricht nicht rechtzeitig bestaetigt: sie wirft eine 503-Ausnahme.
        ResponseStatusException notConfirmed = new ResponseStatusException(
                HttpStatus.SERVICE_UNAVAILABLE, "Kafka hat nicht bestaetigt");
        when(messageService.sendMessage(any())).thenThrow(notConfirmed);

        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "Kommt diese Nachricht an?"
                }
                """;

        // Der Client muss erfahren, dass nichts angenommen wurde - kein 202.
        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isServiceUnavailable());
    }
```

- [ ] **Schritt 2: Tests laufen lassen — sie müssen fehlschlagen**

```bash
cd chat-service
export JAVA_HOME=<Pfad-zu-deinem-JDK-21>   # nur nötig, wenn java -version nicht 21 zeigt
mvn test
```

Erwartet: **FEHLSCHLAG beim Kompilieren** — `sendMessage` gibt es am Service noch nicht.

- [ ] **Schritt 3: Die Kafka-Konfiguration schreiben**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/kafka/KafkaConfiguration.java`:

```java
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
```

> **Warum hier keine Bean für JSON steht — und warum wir nicht den `JsonSerializer` nehmen.**
> Kafka kennt keine Konverter, sondern **Serializer**, und die stehen in `application.yml`
> (Task 2, Schritt 6). Spring Kafka hätte einen fertigen `JsonSerializer`. Wir nehmen trotzdem
> den einfachen `StringSerializer` und wandeln die Nachricht im `MessageService` selbst in
> JSON um, aus zwei Gründen:
>
> 1. Der `JsonSerializer` benutzt **seinen eigenen** `ObjectMapper`, nicht den von Spring Boot.
>    Der schreibt ein `Instant` als Zahl (`1757577900.000000000`) statt als lesbares
>    `2026-09-04T08:05:00Z`.
> 2. Er hängt jeder Nachricht einen Header `__TypeId__` mit dem vollen Klassennamen an
>    (`ch.benedict.m321.chat.message.Message`). Der `batch-service` hat diese Klasse nicht —
>    er ist ein eigener Dienst mit eigenem Paket — und würde beim Lesen scheitern.
>
> Mit JSON als Text steht auf dem Topic genau das, was jeder Dienst lesen kann, ohne die
> Klassen eines anderen Dienstes zu kennen. Das ist das Modulthema: Dienste teilen ein
> **Datenformat**, keinen Code.

- [ ] **Schritt 4: Den Request-Datensatz schreiben**

Datei `chat-service/src/main/java/ch/benedict/m321/chat/message/NewMessage.java`:

```java
package ch.benedict.m321.chat.message;

import java.util.UUID;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Was der Client beim Senden mitschickt. Bewusst NICHT dasselbe wie Message:
 * id und sentAt vergibt der Server, nicht der Client. Wuerde der Client sie
 * mitschicken duerfen, koennte er sich eine fremde Uhrzeit oder eine fremde ID aussuchen.
 */
public record NewMessage(

        @Schema(description = "In welchen Raum die Nachricht gehoert",
                example = "11111111-1111-1111-1111-111111111111")
        UUID roomId,

        @Schema(description = "PLATZHALTER bis Keycloak da ist. Danach kommt der Absender "
                            + "aus dem Token und dieses Feld faellt ersatzlos weg.",
                example = "lernende1")
        String sender,

        @Schema(description = "Der Nachrichtentext", example = "Hallo zusammen")
        String text) {
}
```

- [ ] **Schritt 5: Den Service um das Senden erweitern**

In `MessageService.java`: die Importe ergänzen, dann den Feldblock und den Konstruktor durch die
folgende Fassung **ersetzen** und die neue Methode anfügen.

Zusätzliche Importe:

```java
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import ch.benedict.m321.chat.kafka.KafkaConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.server.ResponseStatusException;
```

Felder und Konstruktor (ersetzen die bisherige Fassung):

```java
    /**
     * So lange warten wir hoechstens auf die Bestaetigung von Kafka, NACHDEM send() zurueckgekehrt
     * ist. Das laeuft NACH einem moeglichen Block innerhalb von send() selbst (bis zu max.block.ms,
     * siehe application.yml) - im ungluecklichsten Fall warten wir also beide Zeitbudgets
     * nacheinander ab, zusammen rund 10 Sekunden.
     */
    private static final int SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MessageRepository messageRepository;

    /**
     * Den ObjectMapper reicht Spring Boot herein. Dessen Jackson-Autokonfiguration schreibt
     * Zeitpunkte serienmaessig als lesbaren ISO-Text (z. B. "2026-09-04T08:05:00Z") statt als
     * Zahl - anders als der JsonSerializer von Spring Kafka mit seinem eigenen ObjectMapper.
     */
    public MessageService(KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper,
                          MessageRepository messageRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.messageRepository = messageRepository;
    }
```

Neue Methode (anfügen):

```java
    /**
     * Nimmt eine neue Nachricht an und gibt sie an Kafka weiter.
     *
     * Wichtig: hier wird NICHT in die Datenbank geschrieben. Der chat-service
     * publiziert nur; gespeichert wird spaeter gebuendelt vom batch-service
     * (PLANUNG.md, Abschnitt 2.3).
     */
    public Message sendMessage(NewMessage incoming) {
        // Die ID vergeben WIR, nicht die Datenbank. Nur so kann der batch-service
        // ein Paket gefahrlos wiederholen, ohne Dubletten zu erzeugen.
        UUID id = UUID.randomUUID();

        // Auch die Zeit setzen wir hier: das ist der Moment des SENDENS.
        // Die Datenbank wuerde spaeter den Moment des SCHREIBENS festhalten.
        Instant sentAt = Instant.now();

        Message message = new Message(id, incoming.roomId(), incoming.sender(),
                incoming.text(), sentAt);

        log.info("Nachricht {} von {} fuer Raum {} wird publiziert",
                id, incoming.sender(), incoming.roomId());

        // Zweites Argument ist der SCHLUESSEL. Kafka rechnet daraus die Partition
        // aus: gleicher Schluessel -> gleiche Partition -> garantierte Reihenfolge.
        // Deshalb steht hier die roomId - alle Nachrichten eines Raums bleiben in
        // der Reihenfolge, in der sie gesendet wurden.
        String partitionKey = incoming.roomId().toString();
        String json = toJson(message);

        // Wir warten auf die Bestaetigung von Kafka. Sonst meldeten wir dem Client
        // "202 angenommen" fuer eine Nachricht, die vielleicht nie angekommen ist.
        SendResult<String, String> result = sendAndWaitForKafka(partitionKey, json, id);

        // Kafka meldet zurueck, WO die Nachricht gelandet ist. Im Log sieht man so,
        // dass alle Nachrichten eines Raums immer in derselben Partition landen.
        RecordMetadata metadata = result.getRecordMetadata();
        log.info("Nachricht {} liegt auf {} in Partition {} an Offset {}",
                id, KafkaConfiguration.TOPIC_NAME, metadata.partition(), metadata.offset());
        return message;
    }

    /**
     * Sendet die Nachricht an Kafka und wartet hoechstens SEND_TIMEOUT_SECONDS auf die
     * Bestaetigung. Scheitert das Senden oder bleibt die Bestaetigung aus, antwortet der
     * chat-service mit 503: der Chat nimmt sichtbar nichts an, statt still Nachrichten zu
     * verlieren (PLANUNG.md, Abschnitt 2.4).
     */
    private SendResult<String, String> sendAndWaitForKafka(String partitionKey, String json, UUID id) {
        try {
            // send() kehrt in aller Regel SOFORT zurueck und liefert nur ein "Versprechen"
            // (CompletableFuture); der eigentliche Versand laeuft im Hintergrund, in einem
            // Thread des Kafka-Clients. Kennt der Producer die Partitionen des Topics aber
            // noch nicht (z. B. beim allerersten Senden nach dem Start), fragt er zuerst den
            // Broker und blockiert dabei bis zu max.block.ms - und kann dann DIREKT HIER, auf
            // dieser Zeile, mit einer Ausnahme scheitern, statt ein Versprechen zurueckzugeben.
            CompletableFuture<SendResult<String, String>> pending =
                    kafkaTemplate.send(KafkaConfiguration.TOPIC_NAME, partitionKey, json);
            return pending.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (KafkaException | ExecutionException | TimeoutException failure) {
            // Es gibt zwei Klassen namens KafkaException: org.apache.kafka.common.KafkaException
            // und org.springframework.kafka.KafkaException. KafkaTemplate wirft die von SPRING,
            // wenn send() wie oben beschrieben sofort scheitert - genau die fangen wir hier ab,
            // zusammen mit einer ausbleibenden Bestaetigung (ExecutionException/TimeoutException).
            log.warn("Nachricht {} wurde von Kafka nicht bestaetigt: {}", id, failure.toString());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Nachricht konnte nicht weitergegeben werden - bitte spaeter erneut senden");
        } catch (InterruptedException interrupted) {
            // Der Thread wurde beim Warten unterbrochen, zum Beispiel beim Herunterfahren.
            // Wir setzen die Unterbrechungs-Markierung wieder, damit der Aufrufer davon erfaehrt.
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Senden wurde unterbrochen");
        }
    }

    /**
     * Wandelt eine Nachricht in JSON-Text um, so wie sie auf dem Topic stehen soll.
     * Jeder andere Dienst kann diesen Text lesen, ohne unsere Klassen zu kennen.
     */
    private String toJson(Message message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException impossible) {
            // Ein record aus UUID, String und Instant laesst sich immer umwandeln.
            // Landet der Code trotzdem hier, ist das ein Programmierfehler, kein Benutzerfehler.
            throw new IllegalStateException(
                    "Nachricht " + message.id() + " liess sich nicht in JSON umwandeln", impossible);
        }
    }
```

> **Warum wir auf `send(...)` warten.** `send` selbst wartet in aller Regel nicht: es gibt sofort
> ein `CompletableFuture` zurück, und der eigentliche Versand läuft im Hintergrund. Kennt der
> Producer die Partitionen des Topics aber noch nicht — zum Beispiel beim allerersten Senden nach
> dem Start, oder wenn `metadata.max.idle.ms` (Standard 5 Minuten) ohne weiteres Senden verstrichen
> ist —, fragt er zuerst synchron beim Broker nach und kann dabei bis zu `max.block.ms` blockieren
> und **direkt an dieser Zeile** mit einer `KafkaException` scheitern, statt ein Versprechen
> zurückzugeben. `sendAndWaitForKafka` fängt deshalb sowohl dieses sofortige Scheitern als auch
> eine erst später über das `CompletableFuture` gemeldete Ausnahme ab. Ohne das würde der Client
> bei ausgefallenem Broker sonst entweder still `202` bekommen (die Nachricht wäre verloren) oder,
> im Fall der `KafkaException`, ein nacktes `500` ohne Erklärung sehen — beides schliesst
> `PLANUNG.md` (Abschnitt 2.4) aus. Der Preis: jede Sendeanfrage dauert so lange, bis Kafka
> bestätigt hat, oder im schlimmsten Fall so lange, bis beide Zeitbudgets nacheinander abgelaufen
> sind (`max.block.ms` und `SEND_TIMEOUT_SECONDS`, zusammen rund 10 Sekunden) — normalerweise sind
> es nur wenige Millisekunden.
>
> **Ehrlich benannt: `503` heisst «nicht bestätigt», nicht «sicher verloren».** Der Kafka-Client
> versucht es im Hintergrund weiter. Kommt die Nachricht nach unserem Zeitlimit doch noch an und
> sendet der Nutzer erneut, steht sie zweimal im Chat — mit zwei verschiedenen UUIDs, die
> `ON CONFLICT` nicht als Dublette erkennt. Das ist das Grundproblem jeder Übertragung über ein
> Netz: bei einer ausbleibenden Antwort weiss der Absender nicht, ob sie unterwegs verloren ging
> oder nur zu spät kam. Die saubere Antwort (der Client vergibt die ID selbst) gehört in einen
> späteren Plan.
>
> **Ausnahme im Stil.** Der Service wirft hier eine `ResponseStatusException` — eigentlich eine
> Klasse aus der Webschicht. Das ist eine bewusste Abkürzung, siehe «Entscheide» unten.

- [ ] **Schritt 6: `MessageService` allein mit Mockito prüfen**

`MessageControllerTest` prüft nur, dass der Controller den Service ruft — der Service selbst wird
dabei durch eine Attrappe ersetzt und läuft nie wirklich. Ob `sendMessage` wirklich das richtige
JSON mit dem richtigen Schlüssel schickt, und ob es auf **beide** Arten reagiert, wie Kafka
scheitern kann (sofort mit einer Ausnahme, oder erst später über das `CompletableFuture`), prüft
erst dieser Test — ganz ohne Spring-Kontext und ohne Docker.

Datei `chat-service/src/test/java/ch/benedict/m321/chat/message/MessageServiceTest.java`:

```java
package ch.benedict.m321.chat.message;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testet MessageService allein, ohne Spring-Kontext und ohne Docker: KafkaTemplate und
 * MessageRepository sind Attrappen (Mockito), der ObjectMapper ist echt und genauso
 * eingestellt wie der, den Spring Boot dem Service normalerweise hereinreicht.
 */
class MessageServiceTest {

    private static final UUID ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    // .json().build() allein reicht NICHT: ohne Spring-Kontext bleibt
    // WRITE_DATES_AS_TIMESTAMPS auf dem Jackson-Standard (an) - genau das schaltet erst
    // Spring Boots JacksonAutoConfiguration ab. Das wird hier von Hand nachgestellt, damit
    // dieser ObjectMapper wie der echte, von Spring Boot injizierte, ISO-Text schreibt.
    private final ObjectMapper objectMapper = Jackson2ObjectMapperBuilder.json()
            .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private final MessageRepository messageRepository = mock(MessageRepository.class);

    private final MessageService messageService =
            new MessageService(kafkaTemplate, objectMapper, messageRepository);

    /**
     * Prueft, dass eine gesendete Nachricht mit der roomId als Kafka-Schluessel ankommt und
     * dass der Wert JSON mit allen Feldern ist, inklusive sentAt als lesbarem ISO-Zeitstempel.
     */
    @Test
    void sendMessageWritesJsonWithRoomIdAsKey() throws Exception {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("chat.messages", 0), 0L, 0, 0L, 0, 0);
        SendResult<String, String> result = new SendResult<>(null, metadata);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(result));

        NewMessage incoming = new NewMessage(ROOM_ID, "lernende1", "Hallo zusammen");
        messageService.sendMessage(incoming);

        // Wir fangen die drei Argumente von send() ab, um sie einzeln zu pruefen.
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), valueCaptor.capture());

        assertEquals("chat.messages", topicCaptor.getValue());
        assertEquals(ROOM_ID.toString(), keyCaptor.getValue());

        JsonNode json = objectMapper.readTree(valueCaptor.getValue());
        assertTrue(json.has("roomId"));
        assertTrue(json.has("sender"));
        assertTrue(json.has("text"));
        assertTrue(json.has("id"));
        assertTrue(json.get("sentAt").isTextual());
        assertTrue(json.get("sentAt").asText().endsWith("Z"));
    }

    /**
     * Prueft die 503-Antwort fuer den Fall, dass Kafka das Scheitern erst SPAETER ueber das
     * CompletableFuture meldet - send() selbst liefert normal zurueck.
     */
    @Test
    void sendMessageAnswers503WhenKafkaFailsLater() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Broker weg")));

        NewMessage incoming = new NewMessage(ROOM_ID, "lernende1", "Kommt das an?");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> messageService.sendMessage(incoming));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.getStatusCode());
    }

    /**
     * Prueft die 503-Antwort fuer den Fall, dass send() SOFORT eine KafkaException wirft (z. B.
     * weil der Producer noch keine Metadaten fuer das Topic hat) - der Service muss das genauso
     * abfangen wie ein spaeteres Scheitern, statt die Ausnahme unbehandelt bis zum Controller
     * durchzulassen (dort wuerde daraus ein HTTP 500 ohne WARN-Log).
     */
    @Test
    void sendMessageAnswers503WhenSendFailsImmediately() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("Send failed"));

        NewMessage incoming = new NewMessage(ROOM_ID, "lernende1", "Kommt das an?");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> messageService.sendMessage(incoming));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.getStatusCode());
    }
}
```

```bash
cd chat-service
mvn test -Dtest=MessageServiceTest
```

Erwartet: **BESTANDEN.** `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Schritt 7: Den Controller um `POST` erweitern**

In `MessageController.java` anfügen. Zusätzliche Importe:

```java
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
```

Zusätzliche Felder, direkt unter `MAX_LIMIT` einfügen:

```java
    /** Obergrenze fuer "sender". Die Spalte in der Datenbank ist VARCHAR(100). */
    private static final int MAX_SENDER_LENGTH = 100;

    /**
     * Obergrenze fuer "text". Ohne diese Grenze koennte ein riesiger Text den Weg bis zu
     * Kafka schaffen und dort erst als max.request.size scheitern - mit einem irrefuehrenden 503.
     */
    private static final int MAX_TEXT_LENGTH = 2000;
```

Neue Methode:

```java
    /**
     * Nimmt eine Nachricht entgegen und gibt sie an Kafka weiter.
     *
     * Die Antwort ist 202 Accepted und nicht 201 Created: wir haben die Nachricht
     * angenommen und weitergegeben, gespeichert ist sie in diesem Moment noch nicht.
     * 201 wuerde etwas versprechen, was noch nicht stimmt.
     */
    @Operation(
            summary = "Nachricht senden",
            description = "Nimmt eine Nachricht an und schreibt sie auf das Kafka-Topic "
                        + "'chat.messages'. Die Antwort kommt, sobald Kafka den Empfang bestaetigt "
                        + "hat. Gespeichert wird die Nachricht kurz danach vom batch-service - sie "
                        + "erscheint also erst mit kleiner Verzoegerung im Verlauf.")
    @ApiResponse(responseCode = "202", description = "Nachricht angenommen und auf das Topic geschrieben")
    @ApiResponse(responseCode = "400", description = "roomId fehlt, sender fehlt oder ist zu lang, Text leer oder laenger als 2000 Zeichen")
    @ApiResponse(responseCode = "503", description = "Kafka hat nicht rechtzeitig bestaetigt - bitte spaeter erneut senden")
    @PostMapping
    public ResponseEntity<Message> send(@RequestBody NewMessage incoming) {

        log.info("Sendeanfrage erhalten: Raum {}, Absender {}", incoming.roomId(), incoming.sender());

        // Eingaben pruefen, bevor irgendetwas den Dienst verlaesst.
        if (incoming.roomId() == null) {
            log.warn("Sendeanfrage ohne roomId abgelehnt");
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "roomId fehlt");
        }
        if (incoming.sender() == null || incoming.sender().isBlank()) {
            log.warn("Sendeanfrage ohne sender fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sender fehlt");
        }
        if (incoming.sender().length() > MAX_SENDER_LENGTH) {
            log.warn("Sendeanfrage mit zu langem sender fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "sender darf hoechstens " + MAX_SENDER_LENGTH + " Zeichen lang sein");
        }
        if (incoming.text() == null || incoming.text().isBlank()) {
            log.warn("Sendeanfrage mit leerem Text fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "text darf nicht leer sein");
        }
        if (incoming.text().length() > MAX_TEXT_LENGTH) {
            log.warn("Sendeanfrage mit zu langem Text fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "text darf hoechstens " + MAX_TEXT_LENGTH + " Zeichen lang sein");
        }

        Message published = messageService.sendMessage(incoming);

        log.info("Sendeanfrage beantwortet: Nachricht {} angenommen", published.id());
        return ResponseEntity.accepted().body(published);
    }
```

> **Achtung, zwei gleichnamige Annotationen.** `@RequestBody` gibt es zweimal: die von Spring
> (`org.springframework.web.bind.annotation.RequestBody`) bindet den JSON-Körper an den Parameter,
> die von Swagger (`io.swagger.v3.oas.annotations.parameters.RequestBody`) beschreibt ihn nur in
> der Dokumentation. Gebraucht wird hier die **von Spring**. Importiert man versehentlich die
> andere, kompiliert alles, aber der Parameter bleibt zur Laufzeit `null`.

- [ ] **Schritt 8: Tests laufen lassen — sie müssen bestehen**

```bash
mvn test
```

Erwartet: **BESTANDEN.** `Tests run: 9, Failures: 0, Errors: 0`.

> **Docker muss laufen.** Seit Task 2 fährt `ChatServiceApplicationTest` die ganze
> Anwendung hoch und braucht dafür Datenbank und Broker. Vorher im
> Projektwurzelverzeichnis `docker compose up -d` ausführen.

- [ ] **Schritt 9: Von Hand prüfen — die Nachricht muss wirklich im Broker landen**

Ein grüner Test beweist nur, dass der Controller den Service ruft. Ob Kafka die Nachricht
bekommt, sieht man nur im Broker.

> **Wichtig für das Verständnis — und ein echter Unterschied zu RabbitMQ:** Ein Kafka-Topic
> **speichert**, was hineingeschrieben wird, auch wenn gerade **niemand** zuhört. Die Nachricht
> bleibt im Log liegen, bis die Aufbewahrungszeit abläuft. Bei RabbitMQ wäre eine Nachricht an
> einen Exchange ohne gebundene Queue **spurlos** verschwunden; hier können wir den Leser
> gemütlich **nach** dem Senden starten und die Nachricht trotzdem noch sehen. Genau deshalb
> braucht es unten auch keine Hilfskonstruktion.

```bash
docker compose up -d
cd chat-service && mvn spring-boot:run
```

1. Prüfen, dass Spring das Topic beim Start angelegt hat:

```bash
docker exec -it m321-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic chat.messages
```

Erwartet: das Topic existiert mit **6** Partitionen und `ReplicationFactor: 1`. Das ist der
Beweis, dass die `NewTopic`-Bean aus Schritt 3 wirklich gewirkt hat.

2. In Swagger UI `POST /api/messages` ausführen mit:

```json
{
  "roomId": "11111111-1111-1111-1111-111111111111",
  "sender": "lernende1",
  "text": "Erste Nachricht durch den Broker"
}
```

3. Antwort muss **202** sein, mit `id` und `sentAt` vom Server gesetzt.

4. Jetzt erst den Leser starten — die Nachricht liegt ja im Log und läuft nicht weg:

```bash
docker exec -it m321-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic chat.messages \
  --from-beginning --property print.key=true
```

Erwartet: eine Zeile mit der `roomId` als Schlüssel, einem Tabulator, und danach lesbarem JSON
mit `"text":"Erste Nachricht durch den Broker"` und einem Zeitpunkt in der Form
`2026-09-04T08:05:00Z`. Mit `Ctrl+C` beenden — der Consumer wartet sonst weiter auf Neues.

> `--from-beginning` heisst: ab dem Anfang des Logs lesen. Lässt man es weg, zeigt der Consumer
> nur, was **ab jetzt** ankommt — genau das Verhalten, das später die Live-Anzeige braucht
> (`auto.offset.reset: latest`, siehe `PLANUNG.md`, Abschnitt 2.4). Man kann beides hier direkt
> ausprobieren: einmal mit, einmal ohne, und zwischendurch eine Nachricht senden.

Erwartet ist ausserdem: `GET /api/messages` liefert diese Nachricht **nicht** — sie steht ja nicht
in der Datenbank. Genau das ist der Beweis, dass Senden und Speichern getrennt sind. Der
`batch-service` schliesst diese Lücke im nächsten Plan.

- [ ] **Schritt 10: Commit**

```bash
cd it3b-m321
git add chat-service/
git commit -m "feat(chat-service): POST /api/messages schreibt auf das Kafka-Topic"
```

---

## Fertig, wenn …

- [ ] `mvn test` im Verzeichnis `chat-service` ist grün (9 Tests)
- [ ] `docker compose up -d` bringt PostgreSQL und Kafka hoch; `/actuator/health` meldet `db` als
      `UP`, und `kafka-topics.sh --list` antwortet ohne Timeout
- [ ] <http://localhost:8080/swagger-ui.html> zeigt den Bereich «Nachrichten» mit **beiden** Endpunkten,
      jeweils mit Beschreibung, Beispielwerten und den Antwortcodes 200/202/400/503
- [ ] `GET /api/messages` liefert die drei Demo-Nachrichten
- [ ] `POST /api/messages` antwortet mit 202, und die Nachricht ist im Broker sichtbar
- [ ] Fünf Commits liegen vor (Dokumente + vier Bau-Schritte)

---

## Übungsaufgabe für die Klasse

Wird der Code an die Lernenden gegeben, **vor dem Austeilen** in
`kafka/KafkaConfiguration.java` den Rumpf von `chatTopic()` entfernen und ersetzen durch:

```java
    @Bean
    public NewTopic chatTopic() {
        // TODO Übung: Ein Topic mit dem Namen aus TOPIC_NAME zurückgeben.
        //             Es soll PARTITIONS Partitionen haben. Weil in docker-compose
        //             nur ein einziger Broker läuft, kann es nicht repliziert werden.
    }
```

Umfang: **zwei bis drei Zeilen.** Gebraucht werden die Konstanten `TOPIC_NAME` und `PARTITIONS`
sowie `TopicBuilder` mit seinen Methoden `name(...)`, `partitions(...)`, `replicas(...)` und
`build()`. Die Frage, welche Zahl bei `replicas` stehen muss, ist der eigentliche Lerninhalt.

Diese Stelle ist mit Absicht gewählt: fehlt die Zeile, **kompiliert die Datei nicht** («missing
return statement»). Die Aufgabe kann also nicht stillschweigend falsch laufen — sie ist entweder
gelöst oder der Fehler steht sofort da. Der Rest des Dienstes bleibt unverändert.

---

## Prüfungsstoff in diesem Plan

| Thema | Wo es im Code steht |
|---|---|
| **Topic und Partitionen** — ein Log, der auch ohne Leser aufbewahrt; jede Consumer-Gruppe bekommt eine eigene Kopie | `KafkaConfiguration`, Schritt 9 in Task 4 |
| **Schlüssel bestimmt die Partition** — gleicher Schlüssel, gleiche Partition, garantierte Reihenfolge | `MessageService.sendMessage` (`roomId` als Schlüssel) |
| **`advertised.listeners`** — der Broker nennt dem Client die Adresse, unter der er erreichbar ist | `docker-compose.yml`, Hinweis in Task 2 |
| **Publizieren statt Schreiben** — warum der Absender-Dienst die Datenbank nicht anfasst | `MessageService.sendMessage` |
| **202 statt 201** — angenommen ist nicht gespeichert | `MessageController.send` |
| **Asynchron senden, trotzdem bestätigen lassen** — `CompletableFuture`, Zeitlimit, `503`, und zwei Wege, wie das Senden scheitern kann (sofort oder erst später) | `MessageService.sendAndWaitForKafka` |
| **ID und Zeitstempel beim Sender** — Voraussetzung fürs spätere Bündeln | `MessageService.sendMessage` |
| **Schichten** — Controller kennt nur den Service, der Service nur das Repository | alle drei Klassen im Paket `message` |
| **Prepared Statements** — Werte als `?`, nie in den SQL-String geklebt | `MessageRepository.findLatest` |
| **Validierung an der Grenze** — prüfen, bevor etwas den Dienst verlässt | `MessageController`, beide Methoden |
| **API-Dokumentation aus dem Code** — was nicht annotiert ist, steht nicht in Swagger | alle `@Operation`/`@Schema`-Annotationen |
| **docker-compose** — Netzwerk, Volumes, veröffentlichte gegen interne Ports | `docker-compose.yml` |

---

## Entscheide in diesem Plan — und was daran diskutabel ist

| Entscheid | Begründung | Wenn es anders sein soll |
|---|---|---|
| **Englische Bezeichner, deutsche Kommentare** | Java, Spring und SQL bringen englisches Vokabular mit; gemischte Bezeichner wie `findeLetzteByRaumId` liest niemand gern. Erklärt wird trotzdem auf Deutsch. | — |
| **Spring Boot 3.5.16, nicht 4.x** | Boot 4 ist draussen, aber für den Unterricht zählt die Menge an Material: praktisch jedes Tutorial, jede Antwort im Netz und die ganze springdoc-2.x-Linie zielen auf Boot 3. springdoc 2.9.0 wird exakt gegen 3.5.16 gebaut. | Umstellen kostet den Wechsel auf springdoc 3.x und eine neuere Spring-Kafka-Linie |
| **`JdbcTemplate` statt JPA/Hibernate** | `CLAUDE.md` verbietet Annotation-Magie. Beim `JdbcTemplate` steht das SQL im Klartext und jede Spalte wird sichtbar in ein Feld übertragen. Ausserdem benutzt der `batch-service` ohnehin `batchUpdate`. | JPA wäre ein eigenes Kapitel — dann aber bewusst als Thema, nicht nebenbei |
| **Methodenreferenz `this::mapRow`** | Kürzer und benannt — man sieht am Namen, was passiert, statt einen Lambda-Rumpf mitten in der Abfrage zu lesen. | Falls Methodenreferenzen im Unterricht noch nicht dran waren: durch `(row, rowNumber) -> mapRow(row, rowNumber)` ersetzen oder eine benannte `RowMapper<Message>`-Klasse anlegen |
| **`record` statt Klasse mit Gettern** | Eine Zeile statt dreissig, und unveränderlich. Kein Lombok nötig. | Falls `record` im Unterricht noch nicht behandelt wurde: `Message` und `NewMessage` als normale Klassen mit Konstruktor und Gettern schreiben — sonst ändert sich nichts |
| **SQL-Init-Skripte statt Flyway** | Das Postgres-Image führt `/docker-entrypoint-initdb.d` von sich aus aus. Kein Werkzeug, kein Namensschema, kein zusätzliches Konzept. | Bei Schema-Änderungen `docker compose down -v` nötig. Sobald das nervt, ist Flyway die Antwort |
| **`TIMESTAMPTZ` statt `TIMESTAMP`** | Ohne Zeitzone geht die Zone beim Speichern verloren, und `Instant` ist genau ein Zeitpunkt in UTC. | — (`PLANUNG.md` ist bereits nachgezogen) |
| **JSON als Text mit `StringSerializer` statt `JsonSerializer`** | Der `JsonSerializer` von Spring Kafka schreibt Zeitpunkte als Zahl und hängt einen `__TypeId__`-Header mit dem Klassennamen des Senders an, an dem der `batch-service` scheitern würde. JSON-Text ist das neutrale Format zwischen Diensten. | `JsonSerializer` mit dem ObjectMapper von Spring Boot und `spring.json.add.type.headers: false` — geht, braucht aber eine eigene `ProducerFactory`-Bean |
| **Auf die Bestätigung von Kafka warten, sonst `503`** | Ohne Warten bekäme der Client bei ausgefallenem Broker `202`, und die Nachricht wäre still verloren — genau das schliesst `PLANUNG.md` 2.4 aus. Gefangen werden dabei **beide** Arten des Scheiterns: sofort als `KafkaException` direkt bei `send(...)`, oder erst später über das `CompletableFuture` (`ExecutionException`/`TimeoutException`) — sonst würde die sofortige Variante als `500` beim Client landen statt als `503`. | Nicht warten und nur loggen (`whenComplete`) — schneller, aber der Client erfährt nie von einem Verlust |
| **`ResponseStatusException` im Service statt in der Webschicht** | Eine Zeile statt einer eigenen Exception-Klasse und eines `@ExceptionHandler`. Für den Bootstrap reicht das. | Eigene `BrokerUnavailableException` im Service, Umwandlung in `503` per `@ExceptionHandler` im Controller — sauberer getrennt, ein Konzept mehr |
| **`sender` im Request-Body** | Es gibt noch kein Token. Das Feld ist in Swagger ausdrücklich als Platzhalter markiert. | Fällt weg, sobald Keycloak steht — der Name kommt dann aus `preferred_username` |
| **Tests nur mit `MockMvc`** | Läuft ohne Docker und prüft genau das, was die API verspricht. Für die Kafka-Fehlerfaelle im Service kommt ein reiner Mockito-Unit-Test dazu (`MessageServiceTest`), ganz ohne Spring-Kontext. | Repository und Broker werden hier von Hand geprüft (Schritt 9). Echte Integrationstests brauchen Testcontainers — eigener Plan |
| **Kein Eltern-POM** | Jeder Dienst ist eigenständig baubar — das ist das Modulthema. | Bei vier Diensten wird die Wiederholung lästig; dann ein Eltern-POM nachziehen |

---

## Danach

Diese Pläne bauen auf dem Bootstrap auf, in dieser Reihenfolge:

1. **`batch-service`** — Consumer-Gruppe `batch-service` auf `chat.messages`, zuerst einzeln
   schreiben, dann bündeln (`batch = "true"`, `ack-mode: MANUAL`) und den Unterschied messen
   (`PLANUNG.md`, Abschnitt 2.3 und 2.4).
2. **Raumverwaltung** — `POST /api/rooms`, Einladen per Benutzername, Mitgliedsprüfung beim Senden
   und Lesen.
3. **Keycloak** — Realm-Import, Token-Prüfung, `sender` aus dem Token.
4. **SSE** — `GET /stream`, eigene flüchtige Consumer-Gruppe je Instanz
   (`auto.offset.reset: latest`), `@KafkaListener`.
5. **Gateway und React-App** — nginx als einziger offener Port, `chat-service` wandert ins Compose
   und die veröffentlichten Ports aus Task 2 werden wieder zu `expose`.
