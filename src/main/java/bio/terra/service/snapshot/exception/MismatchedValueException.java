package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.BadRequestException;

public class MismatchedValueException extends BadRequestException {
  public MismatchedValueException(String message) {
    super(message);
  }

  public MismatchedValueException(String message, Throwable cause) {
    super(message, cause);
  }

  public MismatchedValueException(Throwable cause) {
    super(cause);
  }
}
