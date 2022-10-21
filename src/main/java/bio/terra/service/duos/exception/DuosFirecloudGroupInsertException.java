package bio.terra.service.duos.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class DuosFirecloudGroupInsertException extends InternalServerErrorException {
  public DuosFirecloudGroupInsertException(String message) {
    super(message);
  }

  public DuosFirecloudGroupInsertException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuosFirecloudGroupInsertException(Throwable cause) {
    super(cause);
  }
}
