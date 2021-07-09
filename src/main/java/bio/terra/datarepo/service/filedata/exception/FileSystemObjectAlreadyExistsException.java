package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class FileSystemObjectAlreadyExistsException extends BadRequestException {
  public FileSystemObjectAlreadyExistsException(String message) {
    super(message);
  }

  public FileSystemObjectAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileSystemObjectAlreadyExistsException(Throwable cause) {
    super(cause);
  }
}
