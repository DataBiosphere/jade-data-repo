package bio.terra.service.dataset.exception;

import bio.terra.common.exception.NotFoundException;

public class ControlFileNotFoundException extends NotFoundException {
  public ControlFileNotFoundException(String message) {
    super(message);
  }

  public ControlFileNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ControlFileNotFoundException(Throwable cause) {
    super(cause);
  }
}
