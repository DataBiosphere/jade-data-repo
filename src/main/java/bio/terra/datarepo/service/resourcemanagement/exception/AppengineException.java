package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class AppengineException extends InternalServerErrorException {
  public AppengineException(String message) {
    super(message);
  }

  public AppengineException(String message, Throwable cause) {
    super(message, cause);
  }

  public AppengineException(Throwable cause) {
    super(cause);
  }
}
