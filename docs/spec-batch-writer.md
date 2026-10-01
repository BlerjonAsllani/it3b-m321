# Spezifikation — batch-writer

Modul M321 · Bewertung 1. Der `batch-writer` holt die Nachrichten vom Broker und legt sie
dauerhaft in PostgreSQL ab. Er ist der einzige Dienst, der in die Tabelle `message` schreibt.

**Broker:** Dieses Projekt verwendet **Apache Kafka** statt RabbitMQ (mit der Lehrperson
abgesprochen; Begründung in `PLANUNG.md`, Nachtrag «Wechsel von RabbitMQ zu Kafka»). Wo die
Aufgabenstellung von der Queue `chat.persist` spricht, ist hier das **Topic `chat.persist`**
gemeint, und statt `chat.dlq` das **Topic `chat.dlq`**. Die Namen sind identisch, die Technik
dahinter ist eine andere.

---

## 1. Zweck und Abgrenzung

**Zweck.** Der `chat-service` nimmt Nachrichten an und schreibt sie auf das Topic `chat.persist`.
Dort bleiben sie liegen. Der `batch-writer` liest sie fortlaufend und schreibt sie **gebündelt**
in die Tabelle `message`. Erst danach gilt eine Nachricht als dauerhaft gespeichert.

**Warum gebündelt.** `PLANUNG.md` rechnet mit 1'667 Nachrichten pro Sekunde. So viele einzelne
`INSERT`-Anweisungen pro Sekunde kostet jede für sich einen Netzwerk-Roundtrip und einen eigenen
Commit — die Datenbank ist lange vor der Anwendung am Anschlag. Mit Paketen von bis zu 500 Zeilen
sinkt die Zahl der Schreibvorgänge auf wenige pro Sekunde.

**Nicht Teil dieses Dienstes:**

| Nicht dabei | Warum |
|---|---|
| Chat-Historie lesen (`GET /messages`) | Gehört dem `chat-service`, liest direkt aus der Datenbank |
| Räume, Mitgliedschaften, Keycloak | Eigene Teilprojekte, laut Aufgabenstellung ausgeschlossen |
| web-gateway, load-generator | Laut Aufgabenstellung ausgeschlossen |
| Zeitgesteuertes Archivieren | Späteres Teilprojekt |
| Mehr als eine Partition pro Nachricht beeinflussen | Die Reihenfolge regelt der Schlüssel des Senders |

---

## 2. Vertrag: was genau auf dem Topic ankommt

**Woher ich das weiss:** Der Wert wird im `chat-service` in `MessageService.toJson(...)` erzeugt.
Die Klasse `Message` (ein `record`) wird mit dem `ObjectMapper` von Spring Boot in Text umgewandelt.
Nachgeprüft habe ich es doppelt: mit dem Test `sendMessageWritesJsonWithRoomIdAsKey` (prüft Schlüssel
und jedes Feld) und von Hand auf dem Topic mit `kafka-console-consumer.sh --property print.key=true`.

| Teil | Wert | Bemerkung |
|---|---|---|
| Topic | `chat.persist` | 6 Partitionen, Replikationsfaktor 1 |
| Schlüssel | `roomId` als Text | Gleicher Raum → gleiche Partition → Reihenfolge je Raum garantiert |
| Wert | JSON als UTF-8-Text | Kein Schema-Header, keine Java-Klassennamen |
| Header | `content_type: application/json` (optional) | Wird **nicht** ausgewertet; der Dienst liest jeden Eintrag als JSON |

**Beispiel eines Werts:**

```json
{"id":"aaaaaaaa-0000-0000-0000-000000000001",
 "roomId":"11111111-1111-1111-1111-111111111111",
 "sender":"lernende1",
 "text":"Hallo zusammen",
 "sentAt":"2026-09-11T08:05:00Z"}
```

| Feld | Typ | Pflicht | Herkunft |
|---|---|---|---|
| `id` | UUID | ja | Vom `chat-service` vergeben, **nicht** von der Datenbank |
| `roomId` | UUID | ja | Aus dem Request |
| `sender` | Text, höchstens 100 Zeichen | ja | Aus dem Request (später aus dem Token) |
| `text` | Text, höchstens 2000 Zeichen | ja | Aus dem Request |
| `sentAt` | ISO-Zeitpunkt mit `Z` | ja | Vom `chat-service` gesetzt, Moment des Sendens |

**Zwei Festlegungen des Senders, die dieser Dienst voraussetzt:**

1. Die **ID kommt vom Sender**. Nur deshalb erkennt die Datenbank eine zweite Zustellung derselben
   Nachricht (Abschnitt 3.4). Eine von der Datenbank vergebene ID würde bei jeder Wiederholung eine
   neue Zeile erzeugen.
2. Der **Zeitstempel kommt vom Sender**. Ein `DEFAULT now()` würde den Moment des Schreibens
   festhalten; bei einem Paket von 500 Nachrichten hätten alle dieselbe Uhrzeit.

**Unbekannte Felder** im JSON werden ignoriert, damit der `chat-service` ein Feld ergänzen kann,
ohne dass dieser Dienst bricht.

---

## 3. Verhalten

### 3.1 Normalfall

1. Der Dienst meldet sich mit der Consumer-Gruppe `batch-writer` am Topic an.
2. Er holt bis zu **500** Einträge auf einmal ab (`max-poll-records`), wartet dabei höchstens
   **200 ms** auf genügend Daten (`fetch-max-wait`).
3. Jeden Eintrag liest er als JSON.
4. Alle gültigen Nachrichten schreibt er mit **einem** `batchUpdate` in die Tabelle `message`.
5. **Erst danach** bestätigt er die Leseposition (`acknowledge()`, `ack-mode: manual`).

Daraus folgt die Obergrenze aus Szenario S4: 1000 Nachrichten ergeben höchstens
⌈1000 / 500⌉ = **2 Schreibvorgänge**, weit unter den geforderten 100 Transaktionen.

### 3.2 Nachrichten gehen nie still verloren

Die Leseposition wird **nur** nach erfolgreichem Schreiben gespeichert. Stürzt der Dienst vorher ab
oder wird er gestoppt, liefert Kafka dieselben Einträge nach dem Start erneut (Szenario S4).
Das ist bewusst **at-least-once**: lieber eine Nachricht zweimal verarbeiten als eine verlieren.
Doppelte Verarbeitung ist durch 3.4 unschädlich.

### 3.3 Kaputte Nachricht (Fehlerklasse 1)

Lässt sich ein Eintrag nicht als Nachricht lesen — ungültiges JSON, leerer Eintrag, das JSON-Wort
`null`, oder ein Zeitpunkt nach dem Jahr 9999, den `TIMESTAMPTZ` nie speichern könnte — dann geht
der Eintrag **unverändert** auf das Topic `chat.dlq`, mit dem Grund im Header `error-reason`.
Das Paket wird bestätigt.

**Begründung:** Eine Wiederholung würde nie zu einem anderen Ergebnis führen. Ohne Ablage würde der
Dienst ewig an dieser einen Nachricht hängen und alle folgenden blockieren.

### 3.4 Dieselbe Nachricht zweimal (Fehlerklasse 1b, Szenario S5)

Das `INSERT` endet auf `ON CONFLICT (id) DO NOTHING`. Die zweite Zustellung ändert nichts und
erzeugt keinen Fehler. Es entsteht **genau eine Zeile**, und **nichts** landet auf `chat.dlq`.

**Begründung:** Die Operation wird dadurch **idempotent** — mehrfaches Ausführen hat dieselbe
Wirkung wie einmaliges. Erst das macht die Wiederholung aus 3.2 gefahrlos.

### 3.5 Die Datenbank lehnt eine Zeile ab (Fehlerklasse 2)

Beispiele: unbekannte `roomId` (Fremdschlüssel), `sender` länger als 100 Zeichen, fehlendes
Pflichtfeld. Ein `batchUpdate` scheitert dann **ganz**, obwohl nur eine Zeile schuld ist.

Der Dienst schreibt dieses eine Paket deshalb **einmalig Zeile für Zeile** neu. Nur die Zeilen, die
dabei wieder scheitern, gehen auf `chat.dlq`; alle anderen werden gespeichert. Danach wird bestätigt.

**Begründung:** Ohne diesen Einzelweg würden 499 einwandfreie Nachrichten wegen einer einzigen
kaputten verworfen. Schon geschriebene Zeilen überspringt `ON CONFLICT`, der zweite Durchgang ist
also gefahrlos.

### 3.6 Datenbank nicht erreichbar (Fehlerklasse 3, Szenario S7)

Erkannt an jedem `DataAccessException`, der **keine** Ablehnung einer Zeile ist. Dann:

- Es wird **nicht** bestätigt.
- Nichts geht auf `chat.dlq`.
- Der Fehler wird einmal als `ERROR` gemeldet, danach wiederholt Spring das **ganze Paket alle
  5 Sekunden, ohne Obergrenze** (`DefaultErrorHandler` mit `FixedBackOff(5000, UNLIMITED_ATTEMPTS)`).
- Der Rückstand ist als wachsender Consumer-Lag sichtbar.
- Sobald die Datenbank zurück ist, läuft alles von selbst weiter — **kein Neustart von Hand**.

**Sonderfall: die Datenbank hängt, statt abzulehnen.** Wird Postgres angehalten (`docker pause`)
statt gestoppt, antwortet es gar nicht mehr. Der PostgreSQL-Treiber wirft dann mitten im Paket
keinen Fehler, sondern einen `AssertionError`. Spring Kafka hält jeden `Error` für tödlich und
**stoppt den Listener dauerhaft** — der Dienst wäre still weg, ohne Log-Zeile und ohne
Wiederholung. `MessageWriter.insertBatch` fängt diesen Fall deshalb ab und macht daraus einen
`DataAccessResourceFailureException`, also einen gewöhnlichen Datenbankfehler. Damit greift die
Wiederholung oben. Belegt durch den Test `DatabaseOutageScenarioTest`.

**Begründung:** Springs Standardverhalten gibt nach wenigen Versuchen auf und **überspringt** das
Paket still. Das wäre genau der stille Datenverlust, den `PLANUNG.md` Abschnitt 2.4 ausschliesst.
Zwei Zeitgrenzen sorgen dafür, dass ein Ausfall überhaupt als Fehler ankommt statt ewig zu hängen:
`connection-timeout: 5000` (Warten auf eine Verbindung) und `socketTimeout: 30` (hängende Abfrage).

### 3.7 Das Dead-Letter-Topic ist nicht erreichbar

Scheitert schon das Ablegen auf `chat.dlq`, wird das Paket **nicht** bestätigt und später erneut
verarbeitet. **Begründung:** Sonst wäre die Nachricht weder gespeichert noch abgelegt — endgültig weg.

### 3.8 Zwei Instanzen (Szenario S6)

Beide Instanzen laufen in derselben Consumer-Gruppe `batch-writer`. Kafka verteilt die 6 Partitionen
unter ihnen auf; **jede Partition gehört zu jedem Zeitpunkt genau einer Instanz**. Deshalb bearbeitet
keine Nachricht zwei Instanzen gleichzeitig, und es entstehen keine Duplikate.

Beim Hoch- oder Herunterfahren einer Instanz verteilt Kafka neu (Rebalance). Eine Nachricht, deren
Paket dabei nicht mehr bestätigt wurde, wird von der neuen Besitzerin erneut gelesen — und von
`ON CONFLICT` abgefangen.

**Begründung für die Consumer-Gruppe statt eigener Gruppen pro Instanz:** Zwei Gruppen würden jede
Nachricht **beide** bekommen und doppelt schreiben wollen.

---

## 4. Datenmodell und Konfiguration

### 4.1 Tabelle

```sql
CREATE TABLE message (
    id       UUID         PRIMARY KEY,
    room_id  UUID         NOT NULL REFERENCES room (id),
    sender   VARCHAR(100) NOT NULL,
    text     TEXT         NOT NULL,
    sent_at  TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_message_room_time ON message (room_id, sent_at DESC);
```

| Entscheid | Begründung |
|---|---|
| `id` als Primärschlüssel, vom Sender vergeben | Macht `ON CONFLICT (id) DO NOTHING` und damit die Idempotenz möglich |
| `room_id` mit Fremdschlüssel | Eine Nachricht ohne Raum ist nicht sinnvoll; die Ablehnung ist Fehlerklasse 2 |
| `TIMESTAMPTZ`, kein `DEFAULT now()` | Der Zeitpunkt des Sendens zählt, nicht der des Schreibens |
| Index auf `(room_id, sent_at DESC)` | Passt genau auf die Verlaufsabfrage des `chat-service` |

**Wo das Schema entsteht:** `db/01-schema.sql` wird beim **ersten** Start des Postgres-Containers
aus `/docker-entrypoint-initdb.d` ausgeführt; `db/02-demo-data.sql` legt danach den Demo-Raum an.
Der `batch-writer` legt **kein** Schema an und ändert es nicht.

### 4.2 Umgebungsvariablen

Alle Werte kommen aus `.env`; die Vorlage dafür ist `.env.example` (im Repo, `.env` selbst nicht).

| Variable | Beispiel | Wofür |
|---|---|---|
| `POSTGRES_DB` | `chat` | Name der Datenbank |
| `POSTGRES_USER` | `chat` | Benutzer für Postgres und für beide Dienste |
| `POSTGRES_PASSWORD` | `chat` | Passwort dazu |
| `CHAT_TOPIC` | `chat.persist` | Topic, von dem gelesen wird |
| `DLQ_TOPIC` | `chat.dlq` | Topic für nicht speicherbare Nachrichten |
| `BATCH_SIZE` | `500` | Obergrenze der Nachrichten pro Paket |
| `BATCH_WAIT_MS` | `200` | Wie lange der Broker höchstens auf genügend Daten wartet |
| `RETRY_INTERVAL_MS` | `5000` | Pause zwischen zwei Versuchen bei Datenbankausfall |

Im Compose gesetzt, nicht konfigurierbar: `SPRING_DATASOURCE_URL` auf `postgres:5432`,
`SPRING_KAFKA_BOOTSTRAP_SERVERS` auf `kafka:9092` — beides Adressen **innerhalb** des Netzes
`chat-net`. Kein Dienst veröffentlicht einen Port auf den Host.

---

## 5. Abnahmekriterien

Jede Zeile ist mit dem angegebenen Befehl messbar. `$C` steht für `docker compose`.

| Nr | Kriterium | Befehl | Erwartet |
|---|---|---|---|
| S1 | Tests grün in einem Lauf | `mvn clean test` | `BUILD SUCCESS`, keine fehlgeschlagenen Tests; startet Postgres und Kafka selbst (Testcontainers) |
| S2 | Stack läuft ohne offene Ports | `cp .env.example .env && $C up -d --build` danach `$C ps` | vier Dienste `running`, Spalte PORTS ohne `->` |
| S3 | 1000 Nachrichten gespeichert | `scripts/szenarien.sh s3` | nach ≤ 60 s `count(*) = 1000`, Lag `0` |
| S4 | Nachholen nach Stillstand, wenige Transaktionen | `scripts/szenarien.sh s4` | 1000 Zeilen vorhanden; `xact_commit`-Differenz < 100 |
| S5 | Duplikat erzeugt eine Zeile | `scripts/szenarien.sh s5` | genau `1` Zeile mit dieser `id`, `chat.dlq` unverändert |
| S6 | Zwei Instanzen, keine Doppelten | `$C up -d --scale batch-writer=2` danach `scripts/szenarien.sh s6` | beide Instanzen haben Partitionen, 1000 Zeilen, keine doppelte `id` |
| S7 | Datenbankausfall ohne Verlust | `scripts/szenarien.sh s7` | nach ≤ 90 s alle 300 Zeilen da, kein Neustart von Hand nötig |
| S8 | Codestil | `grep -rn "\.stream()" batch-writer/src/main` und `git ls-files .env` | beides ohne Treffer; Kommentar über jeder Klasse und Methode |

**Messpunkte von innen:**

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM message"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group batch-writer
```
