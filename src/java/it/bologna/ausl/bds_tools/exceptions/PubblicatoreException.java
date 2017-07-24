package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class PubblicatoreException extends Exception {

    public PubblicatoreException() {
    }
    
    public PubblicatoreException(String message) {
        super(message);
    }

    public PubblicatoreException(Throwable cause) {
        super(cause);
    }

    public PubblicatoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
