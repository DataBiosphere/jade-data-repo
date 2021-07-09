package bio.terra.datarepo.grammar.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

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
