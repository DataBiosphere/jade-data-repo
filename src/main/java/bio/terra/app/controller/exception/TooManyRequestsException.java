package bio.terra.app.controller.exception;

import bio.terra.common.exception.ErrorReportException;
import java.util.List;
import org.springframework.http.HttpStatus;

/** This exception maps to HttpStatus.TOO_MANY_REQUESTS in the GlobalExceptionHandler. */
public class TooManyRequestsException extends ErrorReportException {
  public TooManyRequestsException(String message) {
    super(message);
  }

  public TooManyRequestsException(String message, List<String> causes, HttpStatus status) {
    super(message, causes, status);
  }
}
