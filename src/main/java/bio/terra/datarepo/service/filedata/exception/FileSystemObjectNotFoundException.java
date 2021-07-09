package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class FileSystemObjectNotFoundException extends NotFoundException {
  public FileSystemObjectNotFoundException(String message) {
    super(message);
  }

  public FileSystemObjectNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileSystemObjectNotFoundException(Throwable cause) {
    super(cause);
  }
}
