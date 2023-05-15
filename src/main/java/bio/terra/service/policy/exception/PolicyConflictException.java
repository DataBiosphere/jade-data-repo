package bio.terra.service.policy.exception;

import bio.terra.common.exception.ConflictException;

public class PolicyConflictException extends ConflictException {

  public PolicyConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
