package bio.terra.common.exception;

import java.util.List;

/** This exception maps to HttpStatus.BAD_REQUEST in the GlobalExceptionHandler. */
public abstract class BadRequestException extends DataRepoException {
  public BadRequestException(String message) {
    super(message);
  }

  public BadRequestException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadRequestException(Throwable cause) {
    super(cause);
  }

  public BadRequestException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }

  public BadRequestException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
