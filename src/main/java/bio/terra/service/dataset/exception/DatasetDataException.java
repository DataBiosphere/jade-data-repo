package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class DatasetDataException extends InternalServerErrorException {
  public DatasetDataException(String message) {
    super(message);
  }

  public DatasetDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatasetDataException(Throwable cause) {
    super(cause);
  }
}
