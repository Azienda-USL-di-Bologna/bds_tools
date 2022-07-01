package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class UtilityFunctionException extends Exception {

    public UtilityFunctionException() {
    }
    
    public UtilityFunctionException(String message) {
        super(message);
    }

    public UtilityFunctionException(Throwable cause) {
        super(cause);
    }

    public UtilityFunctionException(String message, Throwable cause) {
        super(message, cause);
    }
}
