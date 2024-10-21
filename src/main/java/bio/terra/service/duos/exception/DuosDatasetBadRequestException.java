package bio.terra.service.duos.exception;

import bio.terra.common.exception.BadRequestException;

public class DuosDatasetBadRequestException extends BadRequestException {
  public DuosDatasetBadRequestException(String message) {
    super(message);
  }

  public DuosDatasetBadRequestException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuosDatasetBadRequestException(Throwable cause) {
    super(cause);
  }
}
