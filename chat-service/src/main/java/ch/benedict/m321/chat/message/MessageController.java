package ch.benedict.m321.chat.message;

import java.util.List;
import java.util.UUID;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Die REST-Schnittstelle fuer Nachrichten. Die Annotationen aus io.swagger.v3
 * beschreiben jeden Endpunkt - daraus baut springdoc die Swagger-Oberflaeche.
 * Was hier nicht beschrieben ist, taucht in der Dokumentation auch nicht auf.
 */
@RestController
@RequestMapping("/api/messages")
@Tag(name = "Nachrichten", description = "Nachrichten senden und den Verlauf eines Raums lesen")
public class MessageController {

    private static final Logger log = LoggerFactory.getLogger(MessageController.class);

    /** Obergrenze fuer "limit". Schuetzt die Datenbank vor einer Abfrage ueber Millionen Zeilen. */
    private static final int MAX_LIMIT = 100;

    /** Obergrenze fuer "sender". Die Spalte in der Datenbank ist VARCHAR(100). */
    private static final int MAX_SENDER_LENGTH = 100;

    /**
     * Obergrenze fuer "text". Ohne diese Grenze koennte ein riesiger Text den Weg bis zu
     * Kafka schaffen und dort erst als max.request.size scheitern - mit einem irrefuehrenden 503.
     */
    private static final int MAX_TEXT_LENGTH = 2000;

    private final MessageService messageService;

    public MessageController(MessageService messageService) {
        this.messageService = messageService;
    }

    /**
     * Liefert die letzten Nachrichten eines Raums, neueste zuerst.
     * Gelesen wird direkt aus der Datenbank - dieser Weg laeuft voellig getrennt
     * vom Senden ueber Kafka.
     */
    @Operation(
            summary = "Verlauf eines Raums lesen",
            description = "Gibt die letzten Nachrichten eines Raums zurueck, neueste zuerst. "
                        + "Demo-Raum zum Ausprobieren: 11111111-1111-1111-1111-111111111111")
    @ApiResponse(responseCode = "200", description = "Verlauf, moeglicherweise leer")
    @ApiResponse(responseCode = "400", description = "limit ist kleiner als 1 oder groesser als 100")
    @GetMapping
    public List<Message> history(
            @Parameter(description = "ID des Raums", required = true,
                       example = "11111111-1111-1111-1111-111111111111")
            @RequestParam UUID roomId,

            @Parameter(description = "Wie viele Nachrichten hoechstens (1 bis 100)", example = "50")
            @RequestParam(defaultValue = "50") int limit) {

        log.info("Verlauf abgerufen: Raum {}, limit {}", roomId, limit);

        // Grenzen pruefen, bevor die Zahl in die SQL-Abfrage geht.
        if (limit < 1 || limit > MAX_LIMIT) {
            log.warn("Ungueltiges limit {} fuer Raum {} - Anfrage abgelehnt", limit, roomId);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "limit muss zwischen 1 und " + MAX_LIMIT + " liegen");
        }

        List<Message> history = messageService.loadHistory(roomId, limit);
        log.info("Verlauf geliefert: Raum {}, {} Nachrichten", roomId, history.size());
        return history;
    }

    /**
     * Nimmt eine Nachricht entgegen und gibt sie an Kafka weiter.
     *
     * Die Antwort ist 202 Accepted und nicht 201 Created: wir haben die Nachricht
     * angenommen und weitergegeben, gespeichert ist sie in diesem Moment noch nicht.
     * 201 wuerde etwas versprechen, was noch nicht stimmt.
     */
    @Operation(
            summary = "Nachricht senden",
            description = "Nimmt eine Nachricht an und schreibt sie auf das Kafka-Topic "
                        + "'chat.messages'. Die Antwort kommt, sobald Kafka den Empfang bestaetigt "
                        + "hat. Gespeichert wird die Nachricht kurz danach vom batch-service - sie "
                        + "erscheint also erst mit kleiner Verzoegerung im Verlauf.")
    @ApiResponse(responseCode = "202", description = "Nachricht angenommen und auf das Topic geschrieben")
    @ApiResponse(responseCode = "400", description = "roomId fehlt, sender fehlt oder ist zu lang, Text leer oder laenger als 2000 Zeichen")
    @ApiResponse(responseCode = "503", description = "Kafka hat nicht rechtzeitig bestaetigt - bitte spaeter erneut senden")
    @PostMapping
    public ResponseEntity<Message> send(@RequestBody NewMessage incoming) {

        log.info("Sendeanfrage erhalten: Raum {}, Absender {}", incoming.roomId(), incoming.sender());

        // Eingaben pruefen, bevor irgendetwas den Dienst verlaesst.
        if (incoming.roomId() == null) {
            log.warn("Sendeanfrage ohne roomId abgelehnt");
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "roomId fehlt");
        }
        if (incoming.sender() == null || incoming.sender().isBlank()) {
            log.warn("Sendeanfrage ohne sender fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sender fehlt");
        }
        if (incoming.sender().length() > MAX_SENDER_LENGTH) {
            log.warn("Sendeanfrage mit zu langem sender fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "sender darf hoechstens " + MAX_SENDER_LENGTH + " Zeichen lang sein");
        }
        if (incoming.text() == null || incoming.text().isBlank()) {
            log.warn("Sendeanfrage mit leerem Text fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "text darf nicht leer sein");
        }
        if (incoming.text().length() > MAX_TEXT_LENGTH) {
            log.warn("Sendeanfrage mit zu langem Text fuer Raum {} abgelehnt", incoming.roomId());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "text darf hoechstens " + MAX_TEXT_LENGTH + " Zeichen lang sein");
        }

        Message published = messageService.sendMessage(incoming);

        log.info("Sendeanfrage beantwortet: Nachricht {} angenommen", published.id());
        return ResponseEntity.accepted().body(published);
    }
}
