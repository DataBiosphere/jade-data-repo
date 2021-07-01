package bio.terra.grammar.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class MissingDatasetException extends InternalServerErrorException {

  public MissingDatasetException(String message, Throwable cause) {
    super(message, cause);
  }

  public MissingDatasetException(String message) {
    super(message);
  }

  public MissingDatasetException(Throwable cause) {
    super(cause);
  }
}
