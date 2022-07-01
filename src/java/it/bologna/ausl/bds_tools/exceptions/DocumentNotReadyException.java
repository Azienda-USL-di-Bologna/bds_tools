package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class DocumentNotReadyException extends Exception {

    public DocumentNotReadyException() {
    }
    
    public DocumentNotReadyException(String message) {
        super(message);
    }

    public DocumentNotReadyException(Throwable cause) {
        super(cause);
    }

    public DocumentNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
