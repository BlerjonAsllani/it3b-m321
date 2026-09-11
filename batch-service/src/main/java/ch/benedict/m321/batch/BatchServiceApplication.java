package ch.benedict.m321.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Startpunkt des batch-service. Er hat keine Weboberflaeche: nach dem Start laufen nur die
 * Threads des Kafka-Listeners - und die halten den Prozess am Leben, bis man ihn beendet.
 */
@SpringBootApplication
public class BatchServiceApplication {

    /**
     * Uebergibt die Startklasse an Spring Boot. Alles Weitere - Datenbankverbindung,
     * Kafka-Listener, Konfigurationsdateien - erledigt der Aufruf darunter.
     */
    public static void main(String[] args) {
        SpringApplication.run(BatchServiceApplication.class, args);
    }
}
