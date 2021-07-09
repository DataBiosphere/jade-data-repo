package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class MismatchedBillingProfilesException extends InternalServerErrorException {
  public MismatchedBillingProfilesException(String message) {
    super(message);
  }

  public MismatchedBillingProfilesException(String message, Throwable cause) {
    super(message, cause);
  }

  public MismatchedBillingProfilesException(Throwable cause) {
    super(cause);
  }
}
