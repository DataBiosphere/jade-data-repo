package bio.terra.grammar.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidFilterException extends BadRequestException {

  public InvalidFilterException(String message) {
    super(message);
  }
}
