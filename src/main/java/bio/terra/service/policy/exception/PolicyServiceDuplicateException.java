package bio.terra.service.policy.exception;

import bio.terra.common.exception.BadRequestException;

public class PolicyServiceDuplicateException extends BadRequestException {

  public PolicyServiceDuplicateException(String message) {
    super(message);
  }

  public PolicyServiceDuplicateException(String message, Throwable cause) {
    super(message, cause);
  }
}
