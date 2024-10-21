package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidFileChecksumException extends BadRequestException {
  public InvalidFileChecksumException(String message) {
    super(message);
  }

  public InvalidFileChecksumException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidFileChecksumException(Throwable cause) {
    super(cause);
  }
}
