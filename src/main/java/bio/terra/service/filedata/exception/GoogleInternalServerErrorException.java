package bio.terra.service.filedata.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class GoogleInternalServerErrorException extends InternalServerErrorException {
  public GoogleInternalServerErrorException(String message) {
    super(message);
  }

  public GoogleInternalServerErrorException(String message, Throwable cause) {
    super(message, cause);
  }
}
