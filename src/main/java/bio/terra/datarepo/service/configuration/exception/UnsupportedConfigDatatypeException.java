package bio.terra.datarepo.service.configuration.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class UnsupportedConfigDatatypeException extends InternalServerErrorException {
  public UnsupportedConfigDatatypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnsupportedConfigDatatypeException(String message) {
    super(message);
  }

  public UnsupportedConfigDatatypeException(Throwable cause) {
    super(cause);
  }
}
