package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

// This exception is inspired by the fix to DR-612. FireStore will abort transactions due to high
// concurrency.
//
// I also think it mis-reports index scan results under high concurrency.

public class FileSystemAbortTransactionException extends InternalServerErrorException {
  public FileSystemAbortTransactionException(String message) {
    super(message);
  }

  public FileSystemAbortTransactionException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileSystemAbortTransactionException(Throwable cause) {
    super(cause);
  }
}
