package bio.terra.service.job.exception;

import bio.terra.common.exception.NotFoundException;

public class JobNotFoundException extends NotFoundException {
  public JobNotFoundException(String message) {
    super(message);
  }

  public JobNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public JobNotFoundException(Throwable cause) {
    super(cause);
  }
}
