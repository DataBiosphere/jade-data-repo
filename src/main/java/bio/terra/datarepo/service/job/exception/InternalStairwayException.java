package bio.terra.datarepo.service.job.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class InternalStairwayException extends InternalServerErrorException {
  public InternalStairwayException(String message) {
    super(message);
  }

  public InternalStairwayException(String message, Throwable cause) {
    super(message, cause);
  }

  public InternalStairwayException(Throwable cause) {
    super(cause);
  }
}
