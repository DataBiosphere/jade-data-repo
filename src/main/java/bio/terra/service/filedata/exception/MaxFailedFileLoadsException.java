package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class MaxFailedFileLoadsException extends BadRequestException {
  public MaxFailedFileLoadsException(String message) {
    super(message);
  }

  public MaxFailedFileLoadsException(String message, Throwable cause) {
    super(message, cause);
  }

  public MaxFailedFileLoadsException(Throwable cause) {
    super(cause);
  }
}
