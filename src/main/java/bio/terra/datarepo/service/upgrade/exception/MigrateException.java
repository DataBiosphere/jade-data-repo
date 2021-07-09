package bio.terra.datarepo.service.upgrade.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class MigrateException extends InternalServerErrorException {
  public MigrateException(String message) {
    super(message);
  }

  public MigrateException(String message, Throwable cause) {
    super(message, cause);
  }

  public MigrateException(Throwable cause) {
    super(cause);
  }
}
