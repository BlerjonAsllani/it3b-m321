package ch.benedict.m321.batch.message;

/**
 * Meldet, dass ein Eintrag auf dem Topic kein gueltiges Nachrichten-JSON ist. Eine gepruefte
 * Ausnahme (extends Exception): wer parse() aufruft, MUSS entscheiden, was mit einer kaputten
 * Nachricht passiert - der Compiler laesst ihn das nicht vergessen.
 */
public class InvalidMessageException extends Exception {

    /** Legt die Ausnahme mit einer deutschen Beschreibung des Problems an. */
    public InvalidMessageException(String reason) {
        super(reason);
    }
}
