package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class MissingRowCountsException extends InternalServerErrorException {
  public MissingRowCountsException(String message) {
    super(message);
  }

  public MissingRowCountsException(String message, Throwable cause) {
    super(message, cause);
  }

  public MissingRowCountsException(Throwable cause) {
    super(cause);
  }
}
