package bio.terra.datarepo.common.exception;

import java.util.List;

/** This exception maps to HttpStatus.CONFLICT in the GlobalExceptionHandler. */
public abstract class ConflictException extends DataRepoException {
  public ConflictException(String message) {
    super(message);
  }

  public ConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConflictException(Throwable cause) {
    super(cause);
  }

  public ConflictException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }

  public ConflictException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
