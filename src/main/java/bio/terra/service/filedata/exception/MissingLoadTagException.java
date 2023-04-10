package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class MissingLoadTagException extends BadRequestException {
  public MissingLoadTagException(String message) {
    super(message);
  }

  public MissingLoadTagException(String message, Throwable cause) {
    super(message, cause);
  }

  public MissingLoadTagException(Throwable cause) {
    super(cause);
  }
}
