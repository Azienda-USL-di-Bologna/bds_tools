package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
public class RequestException extends Exception {
private final int httpStatusCode;
    
    public RequestException(int httpStatusCode, String message) {
        super(message);
        this.httpStatusCode = httpStatusCode;
    }
    
    public RequestException(int httpStatusCode, Throwable cause) {
        super(cause);
        this.httpStatusCode = httpStatusCode;
    }
    
    public RequestException(int httpStatusCode, String message, Throwable cause) {
        super(message, cause);
        this.httpStatusCode = httpStatusCode;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }
}
