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

# Laenger als das darf das Warten auf die Datenbank hoechstens dauern - sonst haengt das Skript
# ewig, wenn der batch-service nicht laeuft oder Nachrichten auf dem Dead-Letter-Topic landen
# statt gezaehlt zu werden.
MAX_WAIT_SECONDS=300

# Acht Hex-Ziffern aus der aktuellen Sekunde, dazu vier Hex-Ziffern der Prozess-ID dieses
# Skripts, machen die IDs jedes Laufs eindeutig - auch bei zwei Laeufen in derselben Sekunde
# (die Sekunde allein war nicht kollisionssicher). Sonst wuerde ein zweiter Lauf an ON CONFLICT
# abprallen und waere scheinbar sofort fertig.
RUN_ID=$(printf '%08x-%04x' "$(date +%s)" "$(( $$ % 65536 ))")
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
# Die ID hat das UUID-Format: die 13 Zeichen von RUN_ID (Sekunde + Prozess-ID), dann die
# laufende Nummer.
awk -v count="$COUNT" -v run="$RUN_ID" -v room="$ROOM_ID" -v sentAt="$SENT_AT" 'BEGIN {
  for (i = 1; i <= count; i++) {
    printf "%s\t{\"id\":\"%s-4000-8000-%012d\",\"roomId\":\"%s\",\"sender\":\"lasttest\",\"text\":\"Lastnachricht %d\",\"sentAt\":\"%s\"}\n", room, run, i, room, i, sentAt
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
  # Ohne diese Grenze wuerde das Skript ewig warten, wenn der batch-service gar nicht laeuft
  # oder ein Teil der Nachrichten nie in der Datenbank ankommt, sondern auf dem
  # Dead-Letter-Topic landet (dann zaehlt count_rows sie nie mit).
  WAITED=$(( $(date +%s) - START ))
  if [ "$WAITED" -ge "$MAX_WAIT_SECONDS" ]; then
    echo "Abbruch: nach $WAITED s immer noch nicht alle Nachrichten in der Datenbank" \
         "($((NOW_ROWS - BEFORE)) von $COUNT). Laeuft der batch-service? Liegen Nachrichten" \
         "auf chat.messages-dlt statt in der Datenbank?" >&2
    exit 1
  fi
  # Den Lag nur jede vierte Runde abfragen: das Werkzeug braucht selbst ein bis zwei Sekunden
  # und wuerde sonst die Messung ungenau machen.
  if [ $((LOOP % 4)) -eq 0 ]; then
    echo "  nach $WAITED s: $((NOW_ROWS - BEFORE)) von $COUNT gespeichert, Lag $(current_lag)"
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
