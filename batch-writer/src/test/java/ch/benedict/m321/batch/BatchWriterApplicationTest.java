package ch.benedict.m321.batch;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Faehrt den ganzen batch-writer hoch - aber mit angehaltenem Listener (auto-startup=false) und
 * ohne Kafka-Verbindung: der Test soll nur pruefen, dass alle Teile zusammenpassen. Deshalb
 * legt auto-create=false das Dead-Letter-Topic beim Start nicht an (das braeuchte einen
 * laufenden Broker) und der Test laeuft auch ohne Docker. Mit echtem Broker und echter
 * Datenbank arbeiten die Szenario-Tests.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class BatchWriterApplicationTest {

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
        assertEquals("chat.dlq", deadLetterTopic.name());
        assertEquals(1, deadLetterTopic.numPartitions());
        assertEquals(1, deadLetterTopic.replicationFactor());
        assertNotNull(errorHandler);
    }
}
