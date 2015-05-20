package it.bologna.ausl.bds_tools.exceptions;

/**
 *
 * @author gdm
 */
public class NotAuthorizedException extends Exception {

  /**
   * @param message
   */
  public NotAuthorizedException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public NotAuthorizedException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public NotAuthorizedException(String message, Throwable cause) {
    super(message, cause);
  }
}
