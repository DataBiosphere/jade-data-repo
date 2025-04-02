package bio.terra.service.profile.exception;

import bio.terra.common.exception.BadRequestException;

public class BillingProjectNotAccessibleException extends BadRequestException {
  public BillingProjectNotAccessibleException(String message) {
    super(message);
  }
}
