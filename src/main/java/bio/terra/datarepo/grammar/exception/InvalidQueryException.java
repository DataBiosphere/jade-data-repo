package bio.terra.datarepo.grammar.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class InvalidQueryException extends BadRequestException {

  public InvalidQueryException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidQueryException(String message) {
    super(message);
  }

  public InvalidQueryException(Throwable cause) {
    super(cause);
  }
}
