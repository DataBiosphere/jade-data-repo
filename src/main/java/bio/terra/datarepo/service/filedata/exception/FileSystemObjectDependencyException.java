package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class FileSystemObjectDependencyException extends BadRequestException {
  public FileSystemObjectDependencyException(String message) {
    super(message);
  }

  public FileSystemObjectDependencyException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileSystemObjectDependencyException(Throwable cause) {
    super(cause);
  }
}
