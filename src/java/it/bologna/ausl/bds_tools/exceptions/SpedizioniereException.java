package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class SpedizioniereException extends Exception {

    public SpedizioniereException() {
    }
    
    public SpedizioniereException(String message) {
        super(message);
    }

    public SpedizioniereException(Throwable cause) {
        super(cause);
    }

    public SpedizioniereException(String message, Throwable cause) {
        super(message, cause);
    }
}
