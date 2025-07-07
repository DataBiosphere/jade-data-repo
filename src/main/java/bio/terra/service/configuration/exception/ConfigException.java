package bio.terra.service.configuration.exception;

import bio.terra.common.exception.ErrorReportException;

public class ConfigException extends ErrorReportException {
  public ConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
