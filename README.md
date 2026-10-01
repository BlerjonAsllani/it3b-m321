# M321 — Chat-App (Klasse IT3b)

Lernprojekt zum Modul **M321 Verteilte Systeme / Microservices**. Wir bauen gemeinsam eine
Chat-Anwendung aus mehreren Services, die über einen Message Broker miteinander reden und mit
docker-compose gestartet werden.

## Für Lernende: so startest du

1. Dieses Repository **forken** (Button «Fork» oben rechts).
2. Deinen Fork klonen:
   ```bash
   git clone https://github.com/<dein-benutzername>/it3b-m321.git
   cd it3b-m321
   ```
3. Voraussetzungen installieren: **Java 21**, **Maven**, **Docker Desktop**, **Git**.
4. Die Planung lesen (siehe unten) — erst verstehen, dann programmieren.

Alle Aufgaben werden in **deinem Fork** gelöst. Das Original-Repository bleibt die Referenz.

## Was gebaut wird

| Baustein | Technologie | Aufgabe |
|---|---|---|
| chat-service | Spring Boot 3, Java 21 | REST-API, liefert Nachrichten live per SSE, prüft das Login-Token |
| batch-writer | Spring Boot 3, Java 21 | Einziger Schreiber in die Datenbank, speichert Nachrichten gebündelt |
| gateway | nginx | Einziger nach aussen offener Port, Reverse Proxy |
| keycloak | Keycloak | Login (OIDC) |
| kafka | Apache Kafka | Message Broker zwischen den Services (Topics `chat.persist` und `chat.dlq`) |
| postgres | PostgreSQL | Speichert den Chat-Verlauf |
| Web-UI | React | Browser-Client |
| Desktop-UI | JavaFX | Zweiter Client gegen dieselbe API |

Alles unterhalb des Gateways läuft in einem internen Docker-Netzwerk und ist von aussen nicht
erreichbar.

## Dokumente

- [`PLANUNG.md`](PLANUNG.md) — Stack, Architektur, Nachrichtenfluss, Datenmodell, offene Punkte.
  Das ist die Grundlage für alles Weitere.
- [`docs/design/2026-08-28-chat-app-architektur.html`](docs/design/2026-08-28-chat-app-architektur.html)
  — grafische Fassung der Architekturdiagramme, lokal im Browser öffnen (funktioniert ohne Internet).
- [`docs/plan/2026-09-04-chat-service-bootstrap.md`](docs/plan/2026-09-04-chat-service-bootstrap.md)
  — Schritt-für-Schritt-Plan für den ersten Service: Projekt anlegen, Datenbank und Kafka
  anbinden, Nachrichten lesen und senden. Jeder Schritt mit Test.
- [`docs/spec-batch-writer.md`](docs/spec-batch-writer.md)
  — **Spezifikation des `batch-writer`**: Vertrag des Topics, Verhalten in jedem Fehlerfall,
  Datenmodell, Umgebungsvariablen und messbare Abnahmekriterien.
- [`docs/plan-batch-writer.md`](docs/plan-batch-writer.md)
  — **Umsetzungsplan des `batch-writer`**: Aufgaben in Bau-Reihenfolge, jede mit Test und Commit.
- [`docs/superpowers/specs/2026-09-11-batch-service-design.md`](docs/superpowers/specs/2026-09-11-batch-service-design.md)
  — frühere Fassung des Designs (damals noch `batch-service`): Einzel- und Paketstufe, Fehlerklassen.
- [`docs/plan/2026-09-11-batch-service.md`](docs/plan/2026-09-11-batch-service.md)
  — der Plan dazu, mit der Messung einzeln gegen gebündelt.
- [`CLAUDE.md`](CLAUDE.md) — Codestil-Regeln für dieses Projekt. Gelten auch für dich.
- `docs/skizze-architektur.heic` — die Handskizze aus dem Unterricht, von der die Planung ausgeht.

## Codestil, kurz

Der Massstab ist: **kann eine lernende Person jede Zeile vorlesen und sagen, was sie tut?**

- Eine Anweisung pro Zeile, Zwischenresultate in benannte Variablen.
- `for`-Schleife statt Stream, `if` statt verschachteltem Ternary.
- Sprechende Namen in ganzen Wörtern.
- Über jeder Methode ein bis zwei Sätze: was sie tut und warum es sie gibt.
- Kommentare auf Deutsch, als Erklärung an eine Mitlernende.

Die vollständigen Regeln stehen in [`CLAUDE.md`](CLAUDE.md).

## Stand

| Baustein | Stand |
|---|---|
| `chat-service` | läuft: `POST /messages` (auch `/api/messages`) schreibt auf `chat.persist`, `GET /messages` liest den Verlauf |
| `batch-writer` | läuft: speichert gebündelt in PostgreSQL, Dead-Letter-Topic `chat.dlq`, überlebt Datenbankausfälle |
| `kafka`, `postgres` | laufen im Compose, kein Port nach aussen |
| Räume, SSE, Web-UI, Keycloak, Gateway | noch nicht gebaut — siehe `PLANUNG.md`, Abschnitt 5 |

## Ausprobieren

Voraussetzung: Docker läuft. Java und Maven braucht es nur für die Tests.

1. `cp .env.example .env`
2. `docker compose up -d --build` — startet `postgres`, `kafka`, `chat-service` und `batch-writer`.
3. Eine Nachricht senden. Kein Dienst veröffentlicht einen Port, der Aufruf läuft deshalb von
   innen durch das Netz `chat-net`:

   ```bash
   docker compose exec -T postgres sh -c "wget -q -O- --header='Content-Type: application/json' \
     --post-data='{\"roomId\":\"11111111-1111-1111-1111-111111111111\",\"sender\":\"lernende1\",\"text\":\"Hallo\"}' \
     http://chat-service:8080/messages"
   ```

4. Nachschauen, ob sie gespeichert wurde:

   ```bash
   docker compose exec -T postgres psql -U chat -d chat -c "SELECT sender, text FROM message ORDER BY sent_at DESC LIMIT 5"
   ```

5. Tests: `mvn clean test` im Projektwurzelverzeichnis. Die Tests starten ihre eigene Datenbank
   und ihren eigenen Broker (Testcontainers), der Stack oben muss dafür nicht laufen.
6. Die Szenarien der Bewertung prüfen: `scripts/szenarien.sh alle`
