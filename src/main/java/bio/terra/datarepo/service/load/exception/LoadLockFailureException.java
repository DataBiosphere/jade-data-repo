package bio.terra.service.load.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class LoadLockFailureException extends InternalServerErrorException {
  public LoadLockFailureException(String message) {
    super(message);
  }

  public LoadLockFailureException(String message, Throwable cause) {
    super(message, cause);
  }

  public LoadLockFailureException(Throwable cause) {
    super(cause);
  }
}
