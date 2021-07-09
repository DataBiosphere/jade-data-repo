package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class AzureResourceNotFoundException extends NotFoundException {
  public AzureResourceNotFoundException(String message) {
    super(message);
  }

  public AzureResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public AzureResourceNotFoundException(Throwable cause) {
    super(cause);
  }
}
