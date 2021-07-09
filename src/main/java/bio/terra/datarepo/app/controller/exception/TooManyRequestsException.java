package bio.terra.datarepo.app.controller.exception;

import bio.terra.datarepo.common.exception.DataRepoException;

/** This exception maps to HttpStatus.TOO_MANY_REQUESTS in the GlobalExceptionHandler. */
public class TooManyRequestsException extends DataRepoException {
  public TooManyRequestsException(String message) {
    super(message);
  }
}
