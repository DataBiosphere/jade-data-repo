package bio.terra.datarepo.service.snapshot.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

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
