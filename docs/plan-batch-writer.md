# Umsetzungsplan — batch-writer

Grundlage: `docs/spec-batch-writer.md`. Vorbild für den Aufbau: `docs/plan/2026-09-04-chat-service-bootstrap.md`.

**Ausgangslage.** Der Dienst gibt es fachlich schon: er liest vom Topic, schreibt gebündelt mit
`ON CONFLICT`, kennt den Einzelweg und die endlose Wiederholung (Teilprojekt 1, siehe
`docs/plan/2026-09-11-batch-service.md` und `git log`). Was fehlt, ist die **Verpackung**: ein
Eltern-POM, die Namen aus der Aufgabenstellung, der Betrieb im Compose ohne offene Ports und
Tests, die ihre Infrastruktur selbst mitbringen.

**Warum diese Reihenfolge.** Erst die Bau- und Namensstruktur (Aufgabe 1), weil jeder weitere
Schritt darauf aufsetzt. Dann die Schnittstelle nach aussen (2), dann die Tests (3) und der Betrieb
im Compose (4) — beide brauchen nichts voneinander und wurden deshalb parallel gebaut; im `git log`
steht Compose zuerst, weil der Bau der Images länger lief als die Tests. Danach das Messwerkzeug
(5), dann der beim Testen gefundene Fehler (6) und zuletzt die Dokumentation (7).

---

## Aufgabe 1: Eltern-POM, Umbenennung auf `batch-writer`, Topic-Namen

**Warum zuerst:** `mvn clean test` im Wurzelverzeichnis (S1) braucht ein Eltern-POM, und
`--scale batch-writer=2` (S6) braucht den Namen. Beides ändert Pfade, auf die alles Weitere zeigt.

**Warum in einem Schritt mit den Topic-Namen:** Die umbenannten Klassen tragen die Topic-Namen in
sich. Getrennt committet wäre ein Zwischenstand entstanden, der nicht kompiliert — die Tests prüfen
die Namen, die der Dienst benutzt. Ein Schritt, ein lauffähiger Stand.

- Verzeichnis `batch-service/` nach `batch-writer/` umbenennen (`git mv`, Historie bleibt erhalten),
  ebenso die Startklasse auf `BatchWriterApplication`
- `pom.xml` im Wurzelverzeichnis: Packaging `pom`, Module `chat-service` und `batch-writer`
- `chat.messages` → `chat.persist`, `chat.messages-dlt` → `chat.dlq`
- Namen aus `CHAT_TOPIC` und `DLQ_TOPIC` lesen, mit genau diesen Werten als Vorgabe; ebenso
  `BATCH_SIZE`, `BATCH_WAIT_MS`, `RETRY_INTERVAL_MS`, `BATCH_GROUP_ID`
- **Test:** `mvn clean test` im Wurzelverzeichnis baut beide Module, alle Tests grün; der
  Kontext-Test prüft, dass das Dead-Letter-Topic `chat.dlq` heisst
- **Commit:** `build: Eltern-POM, Umbenennung auf batch-writer und Topic-Namen`

## Aufgabe 2: `POST /messages` im chat-service

**Warum hier:** Szenario S3 sendet an diesen Pfad. Ohne ihn fällt S3 aus, und S4 bis S7 bauen darauf
auf, weil sie alle Nachrichten über diesen Weg erzeugen.

- `@RequestMapping` nimmt zusätzlich `/messages` entgegen, `/api/messages` bleibt gültig
- **Test:** `MessageControllerTest` prüft beide Pfade mit 202
- **Commit:** `feat(chat-service): POST /messages zusaetzlich zu /api/messages`

## Aufgabe 3: Tests bringen ihre Infrastruktur selbst mit

**Warum vor dem Compose-Schritt:** S1 läuft vor S2. Die Tests müssen also in einem frischen Klon
grün sein, **ohne** dass vorher ein Stack läuft. Ausserdem sind sie das Netz für Aufgabe 5.

- Testcontainers für PostgreSQL und Kafka, Schema aus `db/01-schema.sql` beim Start laden
- Bestehenden Datenbanktest darauf umstellen (bisher: laufender Stack nötig)
- **Neu, Duplikat (S5):** dieselbe Nachricht zweimal auf das echte Topic legen → genau eine Zeile,
  nichts auf `chat.dlq`
- **Neu, Ausfall (S7):** Datenbank im Test anhalten, Nachrichten senden, Datenbank starten → alle
  Zeilen da, ohne Zutun
- **Test:** `mvn clean test` grün bei gestopptem Compose-Stack
- **Commit:** `test: Duplikat und Datenbankausfall gegen echte Infrastruktur`

## Aufgabe 4: Beide Dienste im Compose, ohne offene Ports

**Warum nach den Tests:** Ab hier wird am Betrieb geschraubt. Grüne Tests zeigen sofort, ob dabei
etwas kaputtgeht.

- Je ein `Dockerfile` (mehrstufig: bauen mit Maven, ausführen mit JRE 21)
- `docker-compose.yml`: `postgres`, `kafka`, `chat-service`, `batch-writer` im Netz `chat-net`
- Keine `ports:`-Einträge mehr; `KAFKA_ADVERTISED_LISTENERS` auf `kafka:9092`
- `batch-writer` ohne `container_name`, sonst scheitert `--scale`
- `.env.example` mit allen Variablen aus der Spezifikation, `.env` in `.gitignore`
- Start-Reihenfolge über `depends_on` mit Healthcheck
- **Test:** `cp .env.example .env && docker compose up -d --build`, danach `docker compose ps`:
  vier Dienste laufen, keine Spalte PORTS mit `->`
- **Commit:** `feat(infra): beide Dienste im Compose, keine veroeffentlichten Ports`

## Aufgabe 5: Szenarien-Skript

**Warum zuletzt vor der Doku:** Es misst, was die Aufgaben 1 bis 4 gebaut haben, und liefert die
Zahlen für die Abnahmekriterien der Spezifikation.

- `scripts/szenarien.sh s3|s4|s5|s6|s7`, misst von innen mit `psql` und `kafka-consumer-groups.sh`
- S4 zählt zusätzlich `xact_commit` vorher und nachher (Obergrenze 100 Transaktionen)
- **Test:** jedes Teilszenario meldet `BESTANDEN` mit gemessenem Wert
- **Commit:** `test: Skript fuer die Szenarien S3 bis S7`

## Aufgabe 6: Hängende Datenbank überlebt der Dienst ebenfalls

**Warum erst hier:** Dieser Fall ist beim Schreiben des Ausfall-Tests (Aufgabe 3) aufgefallen und
nicht vorher bekannt gewesen: Wird Postgres **angehalten** statt gestoppt, wirft der Treiber einen
`AssertionError`, und Spring Kafka stoppt den Listener dauerhaft.

- `MessageWriter.insertBatch` fängt den `AssertionError` ab und wirft einen
  `DataAccessResourceFailureException` — damit ist es ein gewöhnlicher Datenbankfehler und die
  Wiederholung greift
- **Test:** `DatabaseOutageScenarioTest` hält die Datenbank an, lässt sie wieder laufen und prüft,
  dass alle Nachrichten ankommen, ohne den Dienst neu zu starten
- **Commit:** `fix(batch-writer): haengende Datenbank stoppt den Listener nicht mehr`

## Aufgabe 7: Dokumentation nachführen

- `README.md`: Tabelle «Stand» um den `batch-writer` ergänzen, Startanleitung auf Compose umstellen
- `PLANUNG.md`: Topic-Namen und den Betrieb im Compose nachziehen
- **Commit:** `docs: Stand und Startanleitung auf den batch-writer nachfuehren`

---

## Fertig, wenn

- [ ] `mvn clean test` im Wurzelverzeichnis grün, ohne laufenden Stack
- [ ] `docker compose up -d --build` startet vier Dienste, keiner veröffentlicht einen Port
- [ ] `scripts/szenarien.sh` meldet S3 bis S7 als bestanden
- [ ] `grep -rn "\.stream()" batch-writer/src/main` ohne Treffer, `.env` nicht im Repo
- [ ] Tag `bewertung-1` gesetzt und gepusht
