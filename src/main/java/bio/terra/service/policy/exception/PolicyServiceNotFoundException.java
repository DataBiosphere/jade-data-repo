package bio.terra.service.policy.exception;

import bio.terra.common.exception.NotFoundException;

public class PolicyServiceNotFoundException extends NotFoundException {

  public PolicyServiceNotFoundException(String message) {
    super(message);
  }

  public PolicyServiceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
