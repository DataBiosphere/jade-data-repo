package bio.terra.service.job.exception;

import bio.terra.common.exception.ServiceUnavailableException;

public class JobServiceShutdownException extends ServiceUnavailableException {
  public JobServiceShutdownException(String message) {
    super(message);
  }

  public JobServiceShutdownException(String message, Throwable cause) {
    super(message, cause);
  }

  public JobServiceShutdownException(Throwable cause) {
    super(cause);
  }
}
