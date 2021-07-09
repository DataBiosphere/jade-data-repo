package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class GoogleResourceNotFoundException extends NotFoundException {
  public GoogleResourceNotFoundException(String message) {
    super(message);
  }

  public GoogleResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public GoogleResourceNotFoundException(Throwable cause) {
    super(cause);
  }
}
