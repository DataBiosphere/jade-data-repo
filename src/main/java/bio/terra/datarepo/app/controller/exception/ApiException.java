package bio.terra.datarepo.app.controller.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class ApiException extends InternalServerErrorException {
  public ApiException(String message) {
    super(message);
  }

  public ApiException(String message, Throwable cause) {
    super(message, cause);
  }

  public ApiException(Throwable cause) {
    super(cause);
  }
}
