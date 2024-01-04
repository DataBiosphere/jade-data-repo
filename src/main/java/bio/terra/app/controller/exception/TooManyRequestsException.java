package bio.terra.app.controller.exception;

import bio.terra.common.exception.ErrorReportException;

/** This exception maps to HttpStatus.TOO_MANY_REQUESTS in the GlobalExceptionHandler. */
public class TooManyRequestsException extends ErrorReportException {
  public TooManyRequestsException(String message) {
    super(message);
  }
}
