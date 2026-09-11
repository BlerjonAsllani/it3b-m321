# batch-service — Design

Teilprojekt 1 von 6 der Chat-App (Modul M321). Grundlage: `PLANUNG.md` (Abschnitte 2.1, 2.3, 2.4, 3)
und der umgesetzte `chat-service` (Bootstrap-Plan `docs/plan/2026-09-04-chat-service-bootstrap.md`).

Reihenfolge der Teilprojekte (entschieden am 2026-09-11):
1. **batch-service** (dieses Dokument)
2. Räume und Mitgliedsprüfung
3. Live-Anzeige per SSE
4. Gateway (nginx) und React-Oberfläche, `chat-service` und `batch-service` ins Compose
5. Keycloak-Login
6. Zeitgesteuerte Aufgaben im `batch-service` und JavaFX-Client

---

## 1. Ziel

Der `batch-service` liest die Nachrichten vom Kafka-Topic `chat.messages` und schreibt sie in die
Tabelle `message`. Danach erscheint eine gesendete Nachricht im Verlauf (`GET /api/messages`).
Er ist der **einzige** Dienst, der in `message` schreibt, und läuft **genau einmal**.

Gebaut wird in zwei Stufen, damit die Klasse den Unterschied sieht:

1. **Einzeln:** jede Nachricht ein `INSERT`. Danach messen.
2. **Gebündelt:** bis zu 500 Nachrichten ein `batchUpdate`. Danach wieder messen.

Beide Messergebnisse werden als Tabelle festgehalten (Abschnitt 7).

### Nicht Teil dieses Teilprojekts

| Nicht dabei | Wo es hingehört |
|---|---|
| Mitgliedsprüfung beim Senden | Teilprojekt 2 |
| Archivierung und Statistik (`@Scheduled`) | Teilprojekt 6 |
| Zweite `batch-service`-Instanz | nicht geplant (`PLANUNG.md`: genau eine) |
| Lag-Schwelle mit `503` im `chat-service` (offener Punkt 4) | bleibt offen |
| Betrieb im Compose | Teilprojekt 4; bis dahin läuft der Dienst vom Host aus |

---

## 2. Aufbau

Eigenes Maven-Projekt `batch-service/`, gleich aufgebaut wie `chat-service/`: eigenes `pom.xml`,
kein Eltern-POM, Spring Boot 3.5.16, Java 21, Basis-Paket `ch.benedict.m321.batch`.

**Abhängigkeiten:** `spring-boot-starter-jdbc`, `postgresql` (runtime), `spring-kafka`,
`spring-boot-starter-test`. **Kein Web-Starter** — der Dienst hat keine Web-Oberfläche. Die Threads
des Kafka-Listeners halten den Prozess am Leben.

| Datei | Aufgabe |
|---|---|
| `BatchServiceApplication` | Startpunkt |
| `message/ChatMessage` | `record` mit `id`, `roomId`, `sender`, `text`, `sentAt` — eine **eigene Kopie**, kein geteilter Code mit dem `chat-service`. Die Dienste teilen nur das JSON-Format. |
| `message/MessageParser` | Wandelt den JSON-Text vom Topic in ein `ChatMessage` um und meldet kaputtes JSON |
| `message/MessageWriter` | Nur SQL: `insertOne` und `insertBatch`, beide mit `ON CONFLICT (id) DO NOTHING` |
| `message/MessageListener` | `@KafkaListener` der Gruppe `batch-service`; Stufe 1 einzeln, Stufe 2 gebündelt |
| `message/DeadLetterPublisher` | Schreibt eine nicht speicherbare Nachricht mit Originalschlüssel und -wert auf `chat.messages-dlt`, mit Header `error-reason` |
| `kafka/KafkaConfiguration` | Topic-Bean für `chat.messages-dlt`; Fehlerbehandlung mit endloser Wiederholung (Abschnitt 5) |

### Änderungen ausserhalb von `batch-service/`

- **`docker-compose.yml`:** Kafka bekommt ein benanntes Volume. Ohne Volume löscht
  `docker compose down` das Topic und die gespeicherten Offsets — die Aussage «der `batch-service`
  macht nach einem Neustart dort weiter, wo er aufgehört hat» wäre dann nur bis zum nächsten
  Neuaufbau des Containers wahr.
- **`scripts/load-test.sh`:** Lastskript für die Messung (Abschnitt 7).
- **`docs/messungen/<datum>-einzeln-vs-paket.md`:** Messergebnisse.
- **`PLANUNG.md`:** Korrekturen (Abschnitt 8).

---

## 3. Datenfluss

### Stufe 1 — einzeln

`onMessage(record, acknowledgment)`:
1. JSON mit `MessageParser` lesen.
2. `MessageWriter.insertOne(message)`.
3. `acknowledgment.acknowledge()`.

Eine Nachricht = ein `INSERT` = ein Commit in der Datenbank.

### Stufe 2 — gebündelt (ersetzt Stufe 1)

`onMessages(List<ConsumerRecord<String, String>> records, acknowledgment)`:
1. Jeden Datensatz mit `MessageParser` lesen. Kaputtes JSON geht sofort auf das Dead-Letter-Topic
   und ist nicht Teil des `INSERT`.
2. Die gültigen Nachrichten mit **einem** `MessageWriter.insertBatch(...)` schreiben
   (`JdbcTemplate.batchUpdate`).
3. `acknowledgment.acknowledge()` — erst jetzt, wenn alles in der Datenbank steht.

### Warum ohne ausdrückliche Transaktion

Stürzt der Dienst vor dem `acknowledge()` ab oder scheitert das Paket halb geschrieben, liefert Kafka
das ganze Paket noch einmal. Schon geschriebene Zeilen werden durch `ON CONFLICT (id) DO NOTHING`
übersprungen, weil die ID vom `chat-service` kommt (`PLANUNG.md`, Abschnitt 3). Das Wiederholen ist
also ungefährlich, eine Transaktion um das Paket bringt keine zusätzliche Sicherheit — und ist ein
Konzept weniger, das die Klasse erklären muss.

### SQL

```sql
INSERT INTO message (id, room_id, sender, text, sent_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (id) DO NOTHING
```

`sent_at` wird aus dem `Instant` der Nachricht gesetzt, nie per `now()`.

### Konfiguration (`application.yml`)

Alle Zahlen sind Startwerte und werden mit dem Lastskript nachgestellt.

| Einstellung | Wert | Warum |
|---|---|---|
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/chat?reWriteBatchedInserts=true` | Ohne `reWriteBatchedInserts` schickt der Treiber die 500 Zeilen trotzdem einzeln |
| `spring.kafka.consumer.group-id` | `batch-service` | Dauerhafte Gruppe, merkt sich ihre Position |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | Beim allerersten Start liest die Gruppe das Topic von Anfang an — auch Nachrichten, die gesendet wurden, bevor es den `batch-service` gab |
| `spring.kafka.consumer.enable-auto-commit` | `false` | Offsets committet nur der Listener selbst |
| `spring.kafka.listener.ack-mode` | `manual` | `acknowledge()` erst nach dem Schreiben |
| `spring.kafka.consumer.max-poll-records` | `500` | Obergrenze pro Paket |
| `spring.kafka.consumer.fetch-max-wait` | `200ms` | Höchstens so lange wartet der Broker auf genug Daten |
| `spring.kafka.consumer.fetch-min-size` | `100KB` | Etwa 500 Nachrichten zu ~200 Byte |
| Schlüssel/Wert | `StringDeserializer` / `StringSerializer` (für das DLT) | JSON als Text, wie beim `chat-service` |

`max-poll-records`, `fetch-max-wait` und `fetch-min-size` ergeben zusammen eine **Annäherung** an
«500 Stück oder 200 ms» — keine Garantie (`PLANUNG.md`, Abschnitt 2.3).

JSON wird mit dem `ObjectMapper` von Spring Boot gelesen; `sentAt` kommt als ISO-Text und wird zum
`Instant`.

### Logging

- `INFO` pro Paket: «Paket mit 500 Nachrichten in 12 ms geschrieben» — die Grundlage der Messung.
- `DEBUG` pro Zeile im `MessageWriter`.
- `WARN` für jede Nachricht, die auf das Dead-Letter-Topic geht, mit Grund.
- `ERROR` einmal pro fehlgeschlagenem Versuch bei nicht erreichbarer Datenbank.

---

## 4. Dead-Letter-Topic

- Name `chat.messages-dlt`, 1 Partition, 1 Replikat — seltener Verkehr.
- Angelegt vom `batch-service` über eine `NewTopic`-Bean.
- Jeder Eintrag behält **Originalschlüssel** (`roomId`) und **Originalwert** (JSON-Text) und trägt
  den Header `error-reason` mit dem deutschen Grund.
- Ansehen mit `kafka-console-consumer.sh --topic chat.messages-dlt --from-beginning
  --property print.headers=true`.

---

## 5. Fehlerbehandlung

Jeder Fehler gehört zu genau einer von drei Klassen:

| Klasse | Beispiel | Was passiert | Bestätigt? |
|---|---|---|---|
| **Kaputte Nachricht** | ungültiges JSON | sofort auf das Dead-Letter-Topic, keine Wiederholung | ja |
| **Zeile, die die Datenbank ablehnt** | unbekannter Raum (Fremdschlüssel), `sender` zu lang, fehlendes Feld — `DataIntegrityViolationException` | Paket wird **Zeile für Zeile** neu geschrieben; nur die Zeilen, die dabei scheitern, gehen auf das Dead-Letter-Topic, die anderen sind gespeichert | ja |
| **Infrastruktur weg** | Postgres nicht erreichbar, Verbindungs-Timeout | Listener wirft den Fehler weiter; Spring wiederholt das **ganze Paket alle 5 Sekunden, ohne Ende**; nichts geht auf das Dead-Letter-Topic | nein, erst wenn es klappt |

In Stufe 1 gelten dieselben drei Klassen — das «Paket» besteht dort aus genau einer Nachricht, der
Einzelweg entfällt also.

**Die Faustregel:** «nie» (diese Zeile wird nie speicherbar) → Dead-Letter-Topic. «gerade nicht» (die
Datenbank hat ein Problem) → endlos wiederholen. Das ist die Unterscheidung aus `PLANUNG.md` 2.4, jetzt
im Code.

**Endlose Wiederholung ist Absicht.** Die Standardeinstellung von Spring gibt nach wenigen Versuchen
auf und **überspringt** das Paket still — genau der stille Datenverlust, den `PLANUNG.md` ausschliesst.
Konfiguriert wird ein `DefaultErrorHandler` mit `FixedBackOff(5000 ms, unbegrenzt)`. Während der
Wiederholung pausiert der Consumer, fragt aber weiter beim Broker nach — Kafka hält den Dienst deshalb
nicht für tot und verteilt die Partitionen nicht neu.

**Fällt Kafka aus, während auf das Dead-Letter-Topic geschrieben wird,** fliegt dieser Fehler weiter;
das Paket wird nicht bestätigt und wiederholt. Eine Nachricht ist nie «bestätigt, aber verloren».

**Beim Herunterfahren** beendet der Listener das laufende Paket oder bestätigt es nicht — dann liefert
Kafka es nach dem Neustart erneut, und `ON CONFLICT` verhindert Doppelte.

---

## 6. Tests

**Unit-Tests** (JUnit + Mockito, ohne Spring-Kontext, ohne Docker):
- `MessageParser`: gültiges JSON → `ChatMessage` mit `Instant`; kaputtes JSON → als kaputt erkannt.
- `MessageListener` mit gemocktem `MessageWriter` und `DeadLetterPublisher`, je ein Test pro Zeile der
  Fehlertabelle: Normalfall bestätigt; kaputtes JSON → DLT + bestätigt; eine abgelehnte Zeile im Paket
  → Einzelweg, nur diese Zeile auf das DLT, Paket bestätigt; Datenbank weg → Fehler weitergeworfen,
  **nicht** bestätigt.

**Integrationstest gegen das laufende Postgres** (wie der Kontext-Test im `chat-service`; keine
Testcontainers, weil keine zusätzliche Bibliothek ohne Rückfrage):
- `insertBatch` schreibt die Zeilen; ein zweiter Lauf mit denselben IDs ändert nichts.
- Eine unbekannte `roomId` wirft wirklich `DataIntegrityViolationException`. Die ganze Einteilung der
  Fehler hängt daran — deshalb wird sie gegen die echte Datenbank geprüft, nicht gegen einen Mock.

**Von Hand, mit festgehaltenem Ergebnis:**
- `POST /api/messages` im `chat-service` → die Nachricht erscheint in `GET /api/messages` innerhalb
  von etwa einer Sekunde.
- Nachricht mit unbekannter `roomId` → landet auf `chat.messages-dlt` mit Grund.
- Postgres stoppen → Lag der Gruppe `batch-service` wächst (`kafka-consumer-groups.sh --describe`) →
  Postgres starten → Lag baut sich ab, keine Nachricht fehlt.

---

## 7. Messung

`scripts/load-test.sh <anzahl>`:
1. Erzeugt `<anzahl>` gültige Nachrichten für den Demo-Raum
   `11111111-1111-1111-1111-111111111111`, mit pro Lauf eindeutigen IDs.
2. Schreibt sie über `kafka-console-producer.sh` (mit `roomId` als Schlüssel) auf `chat.messages`.
3. Fragt fortlaufend `SELECT count(*)` ab und zeigt dabei den Lag, bis alle Nachrichten da sind.
4. Gibt Dauer und Nachrichten pro Sekunde aus.

Ablauf: Lauf mit 100 000 Nachrichten nach Stufe 1, gleicher Lauf nach Stufe 2. Beide Ergebnisse
kommen als Tabelle in `docs/messungen/<datum>-einzeln-vs-paket.md`, zusammen mit den verwendeten
Einstellungen. Die Messung ersetzt die geratenen Startwerte in `PLANUNG.md`, offener Punkt 3.

---

## 8. Korrekturen in `PLANUNG.md`

Mit Nachtrag im Abschnitt «Verlauf»:
- **Raumprüfung:** Der `chat-service` prüft beim Senden mit **einer lesenden** Abfrage auf
  `room_member`, ob der Absender Mitglied ist, und antwortet sonst mit `403`/`404`. Verboten im
  Anfrageweg sind nur **schreibende** Datenbankzugriffe. Damit ist der Widerspruch zwischen 2.2
  und 3 aufgelöst. Gebaut wird die Prüfung in Teilprojekt 2; der `batch-service` fängt Nachrichten
  für unbekannte Räume zusätzlich über das Dead-Letter-Topic ab.
- **Abschnitt 2.4:** `@RetryableTopic` wird durch den Einzelweg und die endlose Wiederholung aus
  Abschnitt 5 ersetzt. `@RetryableTopic` wird von Spring Kafka für Batch-Listener nicht unterstützt.
- **Offene Punkte:** Punkt 5 (Einzelweg nach fehlgeschlagenem Paket) ist erledigt. Punkt 3 bekommt
  die gemessenen Werte.
- **Nächste Schritte** (Abschnitt 5) folgen der neuen Reihenfolge der Teilprojekte.
