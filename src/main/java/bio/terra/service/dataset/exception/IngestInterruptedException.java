package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class IngestInterruptedException extends InternalServerErrorException {
  public IngestInterruptedException(String message) {
    super(message);
  }

  public IngestInterruptedException(String message, Throwable cause) {
    super(message, cause);
  }

  public IngestInterruptedException(Throwable cause) {
    super(cause);
  }
}
