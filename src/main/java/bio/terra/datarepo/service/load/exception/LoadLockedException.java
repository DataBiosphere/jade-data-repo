package bio.terra.datarepo.service.load.exception;

import bio.terra.datarepo.common.exception.ConflictException;

public class LoadLockedException extends ConflictException {
  public LoadLockedException(String message) {
    super(message);
  }

  public LoadLockedException(String message, Throwable cause) {
    super(message, cause);
  }

  public LoadLockedException(Throwable cause) {
    super(cause);
  }
}
