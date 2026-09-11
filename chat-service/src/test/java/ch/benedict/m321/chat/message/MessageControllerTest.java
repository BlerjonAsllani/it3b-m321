package ch.benedict.m321.chat.message;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Testet den Controller allein. @WebMvcTest startet nur die Webschicht, nicht die
 * ganze Anwendung - deshalb braucht dieser Test weder Datenbank noch Kafka.
 * Der Service wird durch eine Attrappe (@MockitoBean) ersetzt, die wir steuern.
 */
@WebMvcTest(MessageController.class)
class MessageControllerTest {

    private static final UUID ROOM_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MessageService messageService;

    @Test
    void historyReturnsMessagesAsJson() throws Exception {
        // Die Attrappe soll genau eine Nachricht zurueckgeben.
        Message example = new Message(
                UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"),
                ROOM_ID,
                "lehrperson",
                "Willkommen im Raum Allgemein.",
                Instant.parse("2026-09-04T08:00:00Z"));
        when(messageService.loadHistory(any(), anyInt())).thenReturn(List.of(example));

        mockMvc.perform(get("/api/messages").param("roomId", ROOM_ID.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].sender").value("lehrperson"))
                .andExpect(jsonPath("$[0].text").value("Willkommen im Raum Allgemein."));
    }

    @Test
    void sendAcceptsMessageAndReturns202() throws Exception {
        Message created = new Message(
                UUID.fromString("bbbbbbbb-0000-0000-0000-000000000001"),
                ROOM_ID,
                "lernende1",
                "Hallo zusammen",
                Instant.parse("2026-09-04T08:05:00Z"));
        when(messageService.sendMessage(any())).thenReturn(created);

        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "Hallo zusammen"
                }
                """;

        // 202 Accepted heisst: angenommen und weitergegeben - aber noch nicht gespeichert.
        // Genau das ist bei uns der Fall, denn schreiben wird spaeter der batch-service.
        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value("bbbbbbbb-0000-0000-0000-000000000001"))
                .andExpect(jsonPath("$.text").value("Hallo zusammen"));
    }

    @Test
    void sendRejectsBlankText() throws Exception {
        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "   "
                }
                """;

        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void sendReturns503WhenKafkaDoesNotConfirm() throws Exception {
        // Die Attrappe verhaelt sich so wie der echte Service, wenn Kafka die
        // Nachricht nicht rechtzeitig bestaetigt: sie wirft eine 503-Ausnahme.
        ResponseStatusException notConfirmed = new ResponseStatusException(
                HttpStatus.SERVICE_UNAVAILABLE, "Kafka hat nicht bestaetigt");
        when(messageService.sendMessage(any())).thenThrow(notConfirmed);

        String body = """
                {
                  "roomId": "11111111-1111-1111-1111-111111111111",
                  "sender": "lernende1",
                  "text": "Kommt diese Nachricht an?"
                }
                """;

        // Der Client muss erfahren, dass nichts angenommen wurde - kein 202.
        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isServiceUnavailable());
    }
}
