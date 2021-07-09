package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class AzureResourceException extends InternalServerErrorException {
  public AzureResourceException(String message) {
    super(message);
  }

  public AzureResourceException(String message, Throwable cause) {
    super(message, cause);
  }

  public AzureResourceException(Throwable cause) {
    super(cause);
  }
}
