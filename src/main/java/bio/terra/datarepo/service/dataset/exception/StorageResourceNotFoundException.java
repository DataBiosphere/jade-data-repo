package bio.terra.datarepo.service.dataset.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class StorageResourceNotFoundException extends NotFoundException {
  public StorageResourceNotFoundException(String message) {
    super(message);
  }

  public StorageResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageResourceNotFoundException(Throwable cause) {
    super(cause);
  }
}
