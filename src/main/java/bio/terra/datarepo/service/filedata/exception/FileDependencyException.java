package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class FileDependencyException extends BadRequestException {
  public FileDependencyException(String message) {
    super(message);
  }

  public FileDependencyException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileDependencyException(Throwable cause) {
    super(cause);
  }
}
