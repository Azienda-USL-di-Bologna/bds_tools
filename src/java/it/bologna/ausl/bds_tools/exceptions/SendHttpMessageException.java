package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class SendHttpMessageException extends Exception {

private int httpErrorCode;
private String httpErrorMessage;

  public SendHttpMessageException() {
  }
  
  public SendHttpMessageException(int httpErrorCode) {
      this.httpErrorCode = httpErrorCode;
  }
  
    public SendHttpMessageException(int httpErrorCode, String httpErrorMessage) {
      this.httpErrorCode = httpErrorCode;
      this.httpErrorMessage = httpErrorMessage;
  }

  /**
   * @param message
   */
  public SendHttpMessageException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public SendHttpMessageException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public SendHttpMessageException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getHttpErrorMessage() {
      return httpErrorMessage;
  }
  
  public int getHttpErrorCode() {
      return httpErrorCode;
  }

    @Override
    public String toString() {
        return httpErrorCode + " - " + httpErrorMessage + super.toString();
    }
}
