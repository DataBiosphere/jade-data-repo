package bio.terra.tanagra.exception;

/**
 * Custom exception class for system or internal exceptions. These represent errors that the user
 * cannot fix. (e.g. "Error pulling table schema from BigQuery").
 */
public class SystemException extends RuntimeException {
  /**
   * Constructs an exception with the given message. The cause is set to null.
   *
   * @param message description of error that may help with debugging
   */
  public SystemException(String message) {
    super(message);
  }

  /**
   * Constructs an exception with the given message and cause.
   *
   * @param message description of error that may help with debugging
   * @param cause underlying exception that can be logged for debugging purposes
   */
  public SystemException(String message, Throwable cause) {
    super(message, cause);
  }
}
