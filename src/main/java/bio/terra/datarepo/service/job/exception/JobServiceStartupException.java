package bio.terra.datarepo.service.job.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class JobServiceStartupException extends InternalServerErrorException {
  public JobServiceStartupException(String message) {
    super(message);
  }

  public JobServiceStartupException(String message, Throwable cause) {
    super(message, cause);
  }

  public JobServiceStartupException(Throwable cause) {
    super(cause);
  }
}
