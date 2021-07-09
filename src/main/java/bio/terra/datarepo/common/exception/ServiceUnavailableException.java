package bio.terra.datarepo.common.exception;

import java.util.List;

/** This exception maps to HttpStatus.UNAUTHORIZED in the GlobalExceptionHandler. */
public abstract class ServiceUnavailableException extends DataRepoException {
  public ServiceUnavailableException(String message) {
    super(message);
  }

  public ServiceUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServiceUnavailableException(Throwable cause) {
    super(cause);
  }

  public ServiceUnavailableException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }

  public ServiceUnavailableException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
