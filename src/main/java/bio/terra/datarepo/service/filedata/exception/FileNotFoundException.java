package bio.terra.service.filedata.exception;

import bio.terra.common.exception.NotFoundException;

public class FileNotFoundException extends NotFoundException {
  public FileNotFoundException(String message) {
    super(message);
  }

  public FileNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileNotFoundException(Throwable cause) {
    super(cause);
  }
}
