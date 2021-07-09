package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class BillingServiceException extends InternalServerErrorException {
  public BillingServiceException(String message) {
    super(message);
  }

  public BillingServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  public BillingServiceException(Throwable cause) {
    super(cause);
  }
}
