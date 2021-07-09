package bio.terra.datarepo.service.job.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

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
