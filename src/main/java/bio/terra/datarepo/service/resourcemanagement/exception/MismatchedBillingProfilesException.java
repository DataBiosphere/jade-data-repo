package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

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
