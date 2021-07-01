package bio.terra.service.configuration.exception;

import bio.terra.common.exception.NotFoundException;

public class ConfigNotFoundException extends NotFoundException {
  public ConfigNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigNotFoundException(String message) {
    super(message);
  }

  public ConfigNotFoundException(Throwable cause) {
    super(cause);
  }
}
