package bio.terra.service.filedata.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class TooManyDmlStatementsOutstandingException extends InternalServerErrorException {
  public TooManyDmlStatementsOutstandingException(String message) {
    super(message);
  }

  public TooManyDmlStatementsOutstandingException(String message, Throwable cause) {
    super(message, cause);
  }

  public TooManyDmlStatementsOutstandingException(Throwable cause) {
    super(cause);
  }
}
