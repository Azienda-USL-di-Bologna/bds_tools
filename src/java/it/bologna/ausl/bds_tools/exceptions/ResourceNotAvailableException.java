package it.bologna.ausl.bds_tools.exceptions;

public class ResourceNotAvailableException extends Exception{
    
   /**
   * @param message
   */
  public ResourceNotAvailableException(String message) {
    super(message);
  }
  
  /**
   * @param cause
   */
  public ResourceNotAvailableException(Throwable cause) {
    super(cause);
  }
  
  /**
   * @param message
   * @param cause
   */
  public ResourceNotAvailableException(String message, Throwable cause) {
    super(message, cause);
  }
    
}
