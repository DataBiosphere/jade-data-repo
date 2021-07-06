package bio.terra.service.job.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class JobResponseException extends InternalServerErrorException {
  public JobResponseException(String message) {
    super(message);
  }

  public JobResponseException(String message, Throwable cause) {
    super(message, cause);
  }

  public JobResponseException(Throwable cause) {
    super(cause);
  }
}
