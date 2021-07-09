package bio.terra.datarepo.service.job.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class InvalidResultStateException extends InternalServerErrorException {
  public InvalidResultStateException(String message) {
    super(message);
  }

  public InvalidResultStateException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidResultStateException(Throwable cause) {
    super(cause);
  }
}
