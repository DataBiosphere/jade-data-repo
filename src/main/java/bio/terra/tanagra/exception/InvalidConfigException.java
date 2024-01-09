package bio.terra.tanagra.exception;

/**
 * Custom exception class for invalid config exceptions. These represent errors in the definition of
 * the underlay config, that the user needs to fix (e.g. "entity has no attributes").
 */
public class InvalidConfigException extends RuntimeException {
  /**
   * Constructs an exception with the given message. The cause is set to null.
   *
   * @param message description of error that may help with debugging
   */
  public InvalidConfigException(String message) {
    super(message);
  }

  /**
   * Constructs an exception with the given message and cause.
   *
   * @param message description of error that may help with debugging
   * @param cause underlying exception that can be logged for debugging purposes
   */
  public InvalidConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
