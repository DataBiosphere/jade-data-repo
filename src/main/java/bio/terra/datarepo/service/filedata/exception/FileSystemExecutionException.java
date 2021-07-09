package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class FileSystemExecutionException extends InternalServerErrorException {
  public FileSystemExecutionException(String message) {
    super(message);
  }

  public FileSystemExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileSystemExecutionException(Throwable cause) {
    super(cause);
  }
}
