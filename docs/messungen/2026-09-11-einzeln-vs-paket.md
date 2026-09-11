# Messung: einzeln gegen gebündelt

Gemessen am 2026-09-11 mit `scripts/load-test.sh 100000` gegen den `batch-service`.
Alles auf einem Rechner: Apple M4 Pro, 48 GB Arbeitsspeicher, macOS, Docker Desktop (Docker Engine 29.4.0).
Postgres und Kafka im Compose, `batch-service` vom Host aus.

| Stufe | Nachrichten | Dauer | Nachrichten pro Sekunde |
|---|---|---|---|
| 1 — einzeln (`insertOne`, ein Commit pro Nachricht) | 100 000 | 116 s | 862 |
| 2 — gebündelt (`insertBatch`, bis 500 pro Paket) | — | noch nicht gemessen | — |

Einstellungen: `max-poll-records: 500`, `fetch-max-wait: 200ms`, `fetch-min-size: 100KB`,
JDBC-URL mit `reWriteBatchedInserts=true`, Log-Level `INFO`.

Die Dauer zählt vom Start des Skripts bis zur letzten gespeicherten Nachricht; die Genauigkeit liegt
bei etwa ±2 Sekunden.
