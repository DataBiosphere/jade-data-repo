package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.BadRequestException;

public class InaccessibleBillingAccountException extends BadRequestException {
  public InaccessibleBillingAccountException(String message) {
    super(message);
  }

  public InaccessibleBillingAccountException(String message, Throwable cause) {
    super(message, cause);
  }

  public InaccessibleBillingAccountException(Throwable cause) {
    super(cause);
  }
}
