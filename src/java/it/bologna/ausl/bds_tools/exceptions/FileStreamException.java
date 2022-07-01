package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author user
 */
public class FileStreamException extends Exception {

    public FileStreamException(String message) {
        super(message);
    }

    public FileStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileStreamException(Throwable cause) {
        super(cause);
    }

    public FileStreamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
