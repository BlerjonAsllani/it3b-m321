package ch.benedict.m321.batch;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Faehrt den ganzen batch-service hoch - aber mit angehaltenem Listener (auto-startup=false):
 * der Test soll pruefen, dass alle Teile zusammenpassen, und nicht nebenbei echte Nachrichten
 * vom Topic lesen und in die Datenbank schreiben.
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class BatchServiceApplicationTest {

    @Autowired
    private NewTopic deadLetterTopic;

    @Autowired
    private DefaultErrorHandler errorHandler;

    /**
     * Prueft, dass das Dead-Letter-Topic mit einer Partition und einem Replikat angemeldet ist
     * und dass es eine Fehlerbehandlung gibt, die Spring Boot an den Listener haengt.
     */
    @Test
    void kafkaBeansAreCreated() {
        assertEquals("chat.messages-dlt", deadLetterTopic.name());
        assertEquals(1, deadLetterTopic.numPartitions());
        assertEquals(1, deadLetterTopic.replicationFactor());
        assertNotNull(errorHandler);
    }
}
