# Messung: einzeln gegen gebündelt

Stufe 1 gemessen am 2026-09-11, Stufe 2 am 2026-09-12, jeweils mit `scripts/load-test.sh 100000`
gegen den `batch-service`.
Alles auf einem Rechner: Apple M4 Pro, 48 GB Arbeitsspeicher, macOS, OrbStack (Docker Engine 29.4.0).
Postgres und Kafka im Compose, `batch-service` vom Host aus.

| Stufe | Nachrichten | Dauer | Nachrichten pro Sekunde |
|---|---|---|---|
| 1 — einzeln (`insertOne`, ein Commit pro Nachricht) | 100 000 | 116 s | 862 |
| 2 — gebündelt (`insertBatch`, bis 500 pro Paket) | 100 000 | 7 s | 14 285 |

Einstellungen: `max-poll-records: 500`, `fetch-max-wait: 200ms`, `fetch-min-size: 100KB`,
JDBC-URL mit `reWriteBatchedInserts=true`, Log-Level `INFO`.

Die Dauer zählt vom Start des Skripts bis zur letzten gespeicherten Nachricht; die Genauigkeit liegt
bei etwa ±2 Sekunden.

## Einordnung

Stufe 2 ist rund 17-mal schneller als Stufe 1: 7 statt 116 Sekunden für dieselben 100 000
Nachrichten, 14 285 statt 862 Nachrichten pro Sekunde. Dabei war die Datenbank nicht mehr der
Engpass: Pakete mit bis zu 500 Nachrichten standen laut Log in 12 bis 20 ms geschrieben, der Lauf
dauerte insgesamt kaum länger als das Einspielen der 100 000 Nachrichten aufs Topic selbst (2 s)
plus das Nachziehen des Consumers (rund 5 s) — Engpass war also das Lastskript bzw. Kafka, nicht
der `batch-service`. Für die 100 000 Nachrichten pro Sekunde aus PLANUNG.md, Abschnitt 2.3, heisst
das: eine einzelne Instanz schafft mit gemessenen 14 285 Nachrichten pro Sekunde selbst gebündelt
nur gut ein Siebtel der Zielgrösse und ist damit, wie in PLANUNG.md 2.3 und 2.4 beschrieben, allein
nicht für diese Last ausgelegt — das Bündeln hat aber sein eigentliches Ziel erreicht: die Datenbank
als Engpass zu beseitigen.
