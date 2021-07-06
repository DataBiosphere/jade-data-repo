package bio.terra.service.configuration.exception;

import bio.terra.common.exception.BadRequestException;

public class ConfigNotAFaultException extends BadRequestException {
  public ConfigNotAFaultException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigNotAFaultException(String message) {
    super(message);
  }

  public ConfigNotAFaultException(Throwable cause) {
    super(cause);
  }
}
