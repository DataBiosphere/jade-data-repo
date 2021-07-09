package bio.terra.datarepo.service.configuration.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

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
