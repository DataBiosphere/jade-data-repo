package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidStorageException extends BadRequestException {
  public InvalidStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
