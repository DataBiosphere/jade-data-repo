package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidUserProjectException extends BadRequestException {
  public InvalidUserProjectException(String message) {
    super(message);
  }

  public InvalidUserProjectException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidUserProjectException(Throwable cause) {
    super(cause);
  }
}
