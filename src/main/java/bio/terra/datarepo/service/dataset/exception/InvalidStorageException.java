package bio.terra.datarepo.service.dataset.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class InvalidStorageException extends BadRequestException {
  public InvalidStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
