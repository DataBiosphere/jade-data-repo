package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class InvalidFileSystemObjectTypeException extends NotFoundException {
  public InvalidFileSystemObjectTypeException(String message) {
    super(message);
  }

  public InvalidFileSystemObjectTypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidFileSystemObjectTypeException(Throwable cause) {
    super(cause);
  }
}
