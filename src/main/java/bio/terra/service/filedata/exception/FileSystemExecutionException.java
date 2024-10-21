package bio.terra.service.filedata.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class FileSystemExecutionException extends InternalServerErrorException {

  public FileSystemExecutionException(String message) {
    super(message);
  }

  public FileSystemExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileSystemExecutionException(Throwable cause) {
    super("Unexpected interruption during file system processing", cause);
  }
}
