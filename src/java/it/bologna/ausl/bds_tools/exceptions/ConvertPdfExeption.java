package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class ConvertPdfExeption extends Exception {

      public ConvertPdfExeption() {
    }
    
    public ConvertPdfExeption(String message) {
        super(message);
    }

    public ConvertPdfExeption(Throwable cause) {
        super(cause);
    }

    public ConvertPdfExeption(String message, Throwable cause) {
        super(message, cause);
    }
}
