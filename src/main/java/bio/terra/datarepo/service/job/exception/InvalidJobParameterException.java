package bio.terra.service.job.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidJobParameterException extends BadRequestException {
  public InvalidJobParameterException(String message) {
    super(message);
  }

  public InvalidJobParameterException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidJobParameterException(Throwable cause) {
    super(cause);
  }
}
