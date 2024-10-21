package bio.terra.service.duos.exception;

import bio.terra.common.exception.NotFoundException;

public class DuosDatasetNotFoundException extends NotFoundException {
  public DuosDatasetNotFoundException(String message) {
    super(message);
  }

  public DuosDatasetNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuosDatasetNotFoundException(Throwable cause) {
    super(cause);
  }
}
