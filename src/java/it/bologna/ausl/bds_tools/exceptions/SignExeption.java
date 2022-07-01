package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class SignExeption extends Exception {

    public SignExeption() {
    }
    
    public SignExeption(String message) {
        super(message);
    }

    public SignExeption(Throwable cause) {
        super(cause);
    }

    public SignExeption(String message, Throwable cause) {
        super(message, cause);
    }
}
