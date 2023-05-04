package bio.terra.service.policy.exception;

import bio.terra.common.exception.ConflictException;

import java.util.List;

public class PolicyConflictException extends ConflictException {

  public PolicyConflictException(String message) {
    super(message);
  }

  public PolicyConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  public PolicyConflictException(String message, List<String> causes) {
    super(message, causes);
  }
}
