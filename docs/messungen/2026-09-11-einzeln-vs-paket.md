# Messung: einzeln gegen gebündelt

Stufe 1 gemessen am 2026-09-11, Stufe 2 am 2026-09-12, jeweils mit `scripts/load-test.sh 100000`
gegen den `batch-service`.
Alles auf einem Rechner: Apple M4 Pro, 48 GB Arbeitsspeicher, macOS, OrbStack (Docker Engine 29.4.0).
Postgres und Kafka im Compose, `batch-service` vom Host aus.

| Stufe | Nachrichten | Dauer | Nachrichten pro Sekunde |
|---|---|---|---|
| 1 — einzeln (`insertOne`, ein Commit pro Nachricht) | 100 000 | 116 s | 862 |
| 2 — gebündelt (`insertBatch`, bis 500 pro Paket) | 100 000 | 7 s | 14 285 |
| 2 — gebündelt, Wiederholung am 2026-09-13 | 100 000 | 8 s | 12 500 |

Stufe 1 lässt sich mit Commit `7282d07` nachmessen (Stufe 2 hat den Code ersetzt).

Einstellungen: `max-poll-records: 500`, `fetch-max-wait: 200ms`, `fetch-min-size: 100KB`,
JDBC-URL mit `reWriteBatchedInserts=true`, Log-Level `INFO`.

Die Dauer zählt vom Start des Skripts bis zur letzten gespeicherten Nachricht; die Genauigkeit liegt
bei etwa ±2 Sekunden.

## Einordnung

> Stufe 2 ist rund 15-mal schneller als Stufe 1: 7 bzw. 8 Sekunden statt 116 Sekunden für dieselben
> 100 000 Nachrichten, also etwa 12 500–14 300 statt 862 Nachrichten pro Sekunde (Genauigkeit etwa
> ±2 Sekunden). Die Datenbank ist damit nicht mehr der Engpass (ein Paket mit 500 Nachrichten stand
> in 12–40 ms in der Datenbank). Wo die Grenze jetzt liegt — beim Lastskript, bei Kafka oder beim
> `batch-service` selbst —, zeigt diese Messung nicht; dafür müsste man die Last so weit erhöhen,
> dass der Lag dauerhaft wächst. Für die 100 000 Nachrichten pro Sekunde aus `PLANUNG.md`,
> Abschnitt 2.3, heisst das nur: eine einzelne Instanz hat in diesem Lauf rund ein Siebtel bis ein
> Achtel davon geschafft; ob sie mehr kann, ist offen.
