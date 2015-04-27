package it.bologna.ausl.bds_tools.ioda.utils.exceptions;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
public class IodaFileException extends Exception {

    public IodaFileException() {
    }
    
    public IodaFileException(String message) {
        super(message);
    }

    public IodaFileException(Throwable cause) {
        super(cause);
    }

    public IodaFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
