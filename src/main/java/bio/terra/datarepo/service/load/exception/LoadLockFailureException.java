package bio.terra.datarepo.service.load.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

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
