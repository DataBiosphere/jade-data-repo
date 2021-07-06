package bio.terra.service.job.exception;

import bio.terra.common.exception.InternalServerErrorException;

// Exception for failures serializing and deserializing exceptions
public class ExceptionSerializerException extends InternalServerErrorException {
  public ExceptionSerializerException(String message) {
    super(message);
  }

  public ExceptionSerializerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ExceptionSerializerException(Throwable cause) {
    super(cause);
  }
}
