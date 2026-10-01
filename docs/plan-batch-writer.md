# Umsetzungsplan — batch-writer

Grundlage: `docs/spec-batch-writer.md`. Vorbild für den Aufbau: `docs/plan/2026-09-04-chat-service-bootstrap.md`.

**Ausgangslage.** Der Dienst gibt es fachlich schon: er liest vom Topic, schreibt gebündelt mit
`ON CONFLICT`, kennt den Einzelweg und die endlose Wiederholung (Teilprojekt 1, siehe
`docs/plan/2026-09-11-batch-service.md` und `git log`). Was fehlt, ist die **Verpackung**: ein
Eltern-POM, die Namen aus der Aufgabenstellung, der Betrieb im Compose ohne offene Ports und
Tests, die ihre Infrastruktur selbst mitbringen.

**Warum diese Reihenfolge.** Erst die Bau- und Namensstruktur (Aufgabe 1 und 2), weil jeder weitere
Schritt darauf aufsetzt. Dann die Schnittstelle nach aussen (3), dann die Tests (4) — sie sind das
Netz für alles Folgende. Erst danach der Betrieb im Compose (5), weil sich die Szenarien ohne
laufenden Stack nicht messen lassen, und zuletzt das Messwerkzeug (6) und die Dokumentation (7).

---

## Aufgabe 1: Eltern-POM und Umbenennung auf `batch-writer`

**Warum zuerst:** `mvn clean test` im Wurzelverzeichnis (S1) braucht ein Eltern-POM, und
`--scale batch-writer=2` (S6) braucht den Namen. Beides ändert Pfade, auf die alles Weitere zeigt.

- Verzeichnis `batch-service/` nach `batch-writer/` umbenennen (`git mv`, Historie bleibt erhalten)
- `pom.xml` im Wurzelverzeichnis: Packaging `pom`, Module `chat-service` und `batch-writer`
- `artifactId` und `name` des Moduls auf `batch-writer`
- **Test:** `mvn clean test` im Wurzelverzeichnis baut beide Module, alle Tests grün
- **Commit:** `build: Eltern-POM und Umbenennung auf batch-writer`

## Aufgabe 2: Topics `chat.persist` und `chat.dlq`, aus der Umgebung konfigurierbar

**Warum hier:** Die Namen stehen in der Aufgabenstellung und tauchen in beiden Diensten auf. Je
früher sie stimmen, desto weniger muss später angefasst werden.

- `chat.messages` → `chat.persist`, `chat.messages-dlt` → `chat.dlq`
- Namen aus `CHAT_TOPIC` und `DLQ_TOPIC` lesen, mit genau diesen Werten als Vorgabe
- Ebenso `BATCH_SIZE`, `BATCH_WAIT_MS`, `RETRY_INTERVAL_MS`
- **Test:** Kontext-Test prüft, dass die Topic-Beans die Namen aus der Konfiguration tragen
- **Commit:** `feat: Topics chat.persist und chat.dlq aus der Umgebung lesen`

## Aufgabe 3: `POST /messages` im chat-service

**Warum hier:** Szenario S3 sendet an diesen Pfad. Ohne ihn fällt S3 aus, und S4 bis S7 bauen darauf
auf, weil sie alle Nachrichten über diesen Weg erzeugen.

- `@RequestMapping` nimmt zusätzlich `/messages` entgegen, `/api/messages` bleibt gültig
- **Test:** `MessageControllerTest` prüft beide Pfade mit 202
- **Commit:** `feat(chat-service): POST /messages zusaetzlich zu /api/messages`

## Aufgabe 4: Tests bringen ihre Infrastruktur selbst mit

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

## Aufgabe 5: Beide Dienste im Compose, ohne offene Ports

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

## Aufgabe 6: Szenarien-Skript

**Warum zuletzt vor der Doku:** Es misst, was die Aufgaben 1 bis 5 gebaut haben, und liefert die
Zahlen für die Abnahmekriterien der Spezifikation.

- `scripts/szenarien.sh s3|s4|s5|s6|s7`, misst von innen mit `psql` und `kafka-consumer-groups.sh`
- S4 zählt zusätzlich `xact_commit` vorher und nachher (Obergrenze 100 Transaktionen)
- **Test:** jedes Teilszenario meldet `BESTANDEN` mit gemessenem Wert
- **Commit:** `test: Skript fuer die Szenarien S3 bis S7`

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
