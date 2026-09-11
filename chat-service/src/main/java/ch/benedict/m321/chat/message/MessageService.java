package ch.benedict.m321.chat.message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import ch.benedict.m321.chat.kafka.KafkaConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Fachlogik rund um Nachrichten. Der Controller kennt nur diese Klasse, nicht das
 * Repository und nicht Kafka - so bleibt die Weboberflaeche von der Technik
 * dahinter getrennt.
 */
@Service
public class MessageService {

    private static final Logger log = LoggerFactory.getLogger(MessageService.class);

    /** So lange warten wir hoechstens auf die Bestaetigung von Kafka. Passt zu max.block.ms. */
    private static final int SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MessageRepository messageRepository;

    /**
     * Den ObjectMapper reicht Spring Boot herein. Er ist so eingestellt, dass Zeitpunkte
     * als lesbarer Text geschrieben werden - siehe Hinweis in Schritt 3.
     */
    public MessageService(KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper,
                          MessageRepository messageRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.messageRepository = messageRepository;
    }

    /**
     * Liefert den Verlauf eines Raums. Reines Lesen - hier wird nichts veraendert.
     */
    public List<Message> loadHistory(UUID roomId, int limit) {
        log.debug("Verlauf angefordert: Raum {}, hoechstens {} Nachrichten", roomId, limit);
        return messageRepository.findLatest(roomId, limit);
    }

    /**
     * Nimmt eine neue Nachricht an und gibt sie an Kafka weiter.
     *
     * Wichtig: hier wird NICHT in die Datenbank geschrieben. Der chat-service
     * publiziert nur; gespeichert wird spaeter gebuendelt vom batch-service
     * (PLANUNG.md, Abschnitt 2.3).
     */
    public Message sendMessage(NewMessage incoming) {
        // Die ID vergeben WIR, nicht die Datenbank. Nur so kann der batch-service
        // ein Paket gefahrlos wiederholen, ohne Dubletten zu erzeugen.
        UUID id = UUID.randomUUID();

        // Auch die Zeit setzen wir hier: das ist der Moment des SENDENS.
        // Die Datenbank wuerde spaeter den Moment des SCHREIBENS festhalten.
        Instant sentAt = Instant.now();

        Message message = new Message(id, incoming.roomId(), incoming.sender(),
                incoming.text(), sentAt);

        log.info("Nachricht {} von {} fuer Raum {} wird publiziert",
                id, incoming.sender(), incoming.roomId());

        // Zweites Argument ist der SCHLUESSEL. Kafka rechnet daraus die Partition
        // aus: gleicher Schluessel -> gleiche Partition -> garantierte Reihenfolge.
        // Deshalb steht hier die roomId - alle Nachrichten eines Raums bleiben in
        // der Reihenfolge, in der sie gesendet wurden.
        String partitionKey = incoming.roomId().toString();
        String json = toJson(message);

        // send() kehrt sofort zurueck und liefert nur ein "Versprechen" (CompletableFuture).
        // Der eigentliche Versand laeuft im Hintergrund, in einem Thread des Kafka-Clients.
        CompletableFuture<SendResult<String, String>> pending =
                kafkaTemplate.send(KafkaConfiguration.TOPIC_NAME, partitionKey, json);

        // Wir warten trotzdem auf die Bestaetigung. Sonst meldeten wir dem Client
        // "202 angenommen" fuer eine Nachricht, die vielleicht nie angekommen ist.
        SendResult<String, String> result = waitForKafka(pending, id);

        // Kafka meldet zurueck, WO die Nachricht gelandet ist. Im Log sieht man so,
        // dass alle Nachrichten eines Raums immer in derselben Partition landen.
        RecordMetadata metadata = result.getRecordMetadata();
        log.info("Nachricht {} liegt auf {} in Partition {} an Offset {}",
                id, KafkaConfiguration.TOPIC_NAME, metadata.partition(), metadata.offset());
        return message;
    }

    /**
     * Wartet hoechstens SEND_TIMEOUT_SECONDS auf die Bestaetigung von Kafka. Kommt sie
     * nicht, antwortet der chat-service mit 503: der Chat nimmt sichtbar nichts an,
     * statt still Nachrichten zu verlieren (PLANUNG.md, Abschnitt 2.4).
     */
    private SendResult<String, String> waitForKafka(
            CompletableFuture<SendResult<String, String>> pending, UUID id) {
        try {
            return pending.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            log.warn("Nachricht {} wurde von Kafka nicht bestaetigt: {}", id, failure.toString());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Nachricht konnte nicht weitergegeben werden - bitte spaeter erneut senden");
        } catch (InterruptedException interrupted) {
            // Der Thread wurde beim Warten unterbrochen, zum Beispiel beim Herunterfahren.
            // Wir setzen die Unterbrechungs-Markierung wieder, damit der Aufrufer davon erfaehrt.
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Senden wurde unterbrochen");
        }
    }

    /**
     * Wandelt eine Nachricht in JSON-Text um, so wie sie auf dem Topic stehen soll.
     * Jeder andere Dienst kann diesen Text lesen, ohne unsere Klassen zu kennen.
     */
    private String toJson(Message message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException impossible) {
            // Ein record aus UUID, String und Instant laesst sich immer umwandeln.
            // Landet der Code trotzdem hier, ist das ein Programmierfehler, kein Benutzerfehler.
            throw new IllegalStateException(
                    "Nachricht " + message.id() + " liess sich nicht in JSON umwandeln", impossible);
        }
    }
}
