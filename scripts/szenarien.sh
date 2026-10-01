#!/usr/bin/env bash
# Prueft die Szenarien S3 bis S7 der Bewertung 1 gegen den laufenden Stack.
#
# Aufruf aus dem Projektwurzelverzeichnis, der Stack muss laufen:
#   cp .env.example .env && docker compose up -d --build
#   scripts/szenarien.sh s3        (oder s4, s5, s6, s7, oder "alle")
#
# Gemessen wird von innen: psql im Container postgres, kafka-consumer-groups.sh im Container
# kafka. Kein Dienst veroeffentlicht einen Port, deshalb gibt es keinen Weg von aussen.

set -euo pipefail
cd "$(dirname "$0")/.."

# Die Zugangsdaten und Namen stehen in .env - dieselbe Datei, die auch docker compose liest.
set -a
# shellcheck disable=SC1091
source .env
set +a

DEMO_ROOM="11111111-1111-1111-1111-111111111111"

# Zaehlt alle Zeilen der Tabelle message.
count_rows() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM message" | tr -d ' \r'
}

# Zaehlt die Zeilen mit einer bestimmten id - fuer das Duplikat-Szenario.
count_rows_with_id() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM message WHERE id = '$1'" | tr -d ' \r'
}

# Zaehlt alle bestaetigten Transaktionen der Datenbank. Die Differenz vorher/nachher zeigt,
# wie viele Schreibvorgaenge das Buendeln wirklich gespart hat (Szenario S4).
count_transactions() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT xact_commit FROM pg_stat_database WHERE datname = '$POSTGRES_DB'" | tr -d ' \r'
}

# Summe des Lags ueber alle Partitionen der Consumer-Gruppe.
current_lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$BATCH_GROUP_ID" 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'
}

# Zaehlt, wie viele verschiedene Instanzen gerade Partitionen der Gruppe halten (Szenario S6).
count_consumers() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$BATCH_GROUP_ID" 2>/dev/null \
    | awk '$7 ~ /-/ { print $7 }' | sort -u | grep -c . || true
}

# Zaehlt die Eintraege auf dem Dead-Letter-Topic (Summe der Offsets ueber alle Partitionen).
count_dlq() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$DLQ_TOPIC" 2>/dev/null \
    | awk -F: '{ sum += $3 } END { print sum + 0 }'
}

# Schickt <anzahl> Nachrichten an POST /messages. Gesendet wird aus dem Container postgres
# heraus, weil der chat-service nur im Netz chat-net erreichbar ist. Je 25 Anfragen laufen
# gleichzeitig, sonst dauert es zu lange.
send_messages() {
  local count="$1"
  docker compose exec -T postgres sh -c "
    i=1
    while [ \$i -le $count ]; do
      wget -q -O /dev/null --header='Content-Type: application/json' \
        --post-data='{\"roomId\":\"$DEMO_ROOM\",\"sender\":\"pruefung\",\"text\":\"Nachricht \$i\"}' \
        http://chat-service:8080/messages &
      if [ \$((\$i % 25)) -eq 0 ]; then wait; fi
      i=\$((\$i + 1))
    done
    wait
  "
}

# Wartet hoechstens <sekunden>, bis die Tabelle <ziel> Zeilen hat. Gibt die gebrauchte Zeit aus.
wait_for_rows() {
  local target="$1"
  local limit="$2"
  local start now rows
  start=$(date +%s)
  while true; do
    rows=$(count_rows)
    now=$(date +%s)
    if [ "$rows" -ge "$target" ]; then
      echo $((now - start))
      return 0
    fi
    if [ $((now - start)) -ge "$limit" ]; then
      echo "ABBRUCH nach $((now - start)) s: $rows von $target Zeilen" >&2
      return 1
    fi
    sleep 2
  done
}

# Meldet das Ergebnis eines Szenarios in einer Zeile.
report() {
  if [ "$2" = "ja" ]; then
    echo "  $1: BESTANDEN  ($3)"
  else
    echo "  $1: NICHT BESTANDEN  ($3)"
    FAILED=1
  fi
}

FAILED=0

# S3: 1000 Nachrichten ueber POST /messages, nach spaetestens 60 s alle in der Tabelle.
szenario_s3() {
  echo "S3: 1000 Nachrichten ueber POST /messages"
  local before target seconds lag
  before=$(count_rows)
  target=$((before + 1000))
  send_messages 1000
  seconds=$(wait_for_rows "$target" 60) || { report S3 nein "nicht alle innerhalb 60 s"; return; }
  lag=$(current_lag)
  if [ "$lag" -eq 0 ]; then
    report S3 ja "1000 Zeilen nach ${seconds} s, Lag 0"
  else
    report S3 nein "Lag ist $lag statt 0"
  fi
}

# S4: batch-writer gestoppt, 1000 Nachrichten gesendet, dann gestartet. Nichts verloren, und
# die Datenbank fuehrt dafuer hoechstens 100 Transaktionen aus.
szenario_s4() {
  echo "S4: Nachholen nach Stillstand, mit Transaktionszaehler"
  local before target xact_before xact_after used seconds
  docker compose stop batch-writer > /dev/null
  before=$(count_rows)
  target=$((before + 1000))
  send_messages 1000
  xact_before=$(count_transactions)
  docker compose start batch-writer > /dev/null
  seconds=$(wait_for_rows "$target" 90) || { report S4 nein "nicht alle nachgeholt"; return; }
  xact_after=$(count_transactions)
  used=$((xact_after - xact_before))
  if [ "$used" -lt 100 ]; then
    report S4 ja "1000 Zeilen nach ${seconds} s, $used Transaktionen"
  else
    report S4 nein "$used Transaktionen, erlaubt sind unter 100"
  fi
}

# S5: dieselbe Nachricht zweimal direkt auf das Topic. Genau eine Zeile, nichts auf chat.dlq.
szenario_s5() {
  echo "S5: dieselbe Nachricht zweimal direkt auf $CHAT_TOPIC"
  local id json dlq_before dlq_after rows
  id="cccccccc-0000-4000-8000-$(date +%s%N | cut -c1-12)"
  json="{\"id\":\"$id\",\"roomId\":\"$DEMO_ROOM\",\"sender\":\"pruefung\",\"text\":\"Duplikat\",\"sentAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
  dlq_before=$(count_dlq)
  docker compose exec -T kafka sh -c "printf '%s\n%s\n' '$json' '$json' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $CHAT_TOPIC" > /dev/null 2>&1
  sleep 10
  rows=$(count_rows_with_id "$id")
  dlq_after=$(count_dlq)
  if [ "$rows" -eq 1 ] && [ "$dlq_after" -eq "$dlq_before" ]; then
    report S5 ja "genau 1 Zeile, chat.dlq unveraendert bei $dlq_after"
  else
    report S5 nein "$rows Zeilen, chat.dlq $dlq_before -> $dlq_after"
  fi
}

# S6: zwei Instanzen. Beide haengen an der Queue, alle Nachrichten da, keine doppelt.
szenario_s6() {
  echo "S6: zwei Instanzen"
  local before target seconds consumers duplicates
  docker compose up -d --scale batch-writer=2 > /dev/null 2>&1
  sleep 15
  consumers=$(count_consumers)
  before=$(count_rows)
  target=$((before + 1000))
  send_messages 1000
  seconds=$(wait_for_rows "$target" 60) || { report S6 nein "nicht alle gespeichert"; return; }
  duplicates=$(docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM (SELECT id FROM message GROUP BY id HAVING count(*) > 1) AS doppelt" | tr -d ' \r')
  if [ "$consumers" -ge 2 ] && [ "$duplicates" -eq 0 ]; then
    report S6 ja "$consumers Instanzen an der Gruppe, 1000 Zeilen nach ${seconds} s, keine doppelt"
  else
    report S6 nein "$consumers Instanzen, $duplicates doppelte Zeilen"
  fi
  docker compose up -d --scale batch-writer=1 > /dev/null 2>&1
}

# S7: Postgres gestoppt, 300 Nachrichten gesendet, Postgres nach 15 s wieder gestartet.
szenario_s7() {
  echo "S7: Datenbank faellt aus"
  local before target seconds running
  before=$(count_rows)
  target=$((before + 300))
  send_messages 300
  docker compose stop postgres > /dev/null
  sleep 15
  docker compose start postgres > /dev/null
  seconds=$(wait_for_rows "$target" 90) || { report S7 nein "nicht alle nachgeholt"; return; }
  running=$(docker compose ps --status running --format '{{.Service}}' | grep -c batch-writer || true)
  if [ "$running" -ge 1 ]; then
    report S7 ja "300 Zeilen nach ${seconds} s, batch-writer lief durch"
  else
    report S7 nein "batch-writer laeuft nicht mehr"
  fi
}

case "${1:-alle}" in
  s3) szenario_s3 ;;
  s4) szenario_s4 ;;
  s5) szenario_s5 ;;
  s6) szenario_s6 ;;
  s7) szenario_s7 ;;
  alle) szenario_s3; szenario_s4; szenario_s5; szenario_s6; szenario_s7 ;;
  *) echo "Aufruf: $0 [s3|s4|s5|s6|s7|alle]" >&2; exit 1 ;;
esac

exit "$FAILED"
