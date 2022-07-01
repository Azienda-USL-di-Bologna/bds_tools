package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author andrea
 */
public class VersatoreParerException extends Exception {

    public VersatoreParerException() {
    }
    
    public VersatoreParerException(String message) {
        super(message);
    }

    public VersatoreParerException(Throwable cause) {
        super(cause);
    }

    public VersatoreParerException(String message, Throwable cause) {
        super(message, cause);
    }
}
