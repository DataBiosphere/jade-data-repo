package bio.terra.service.common;

import bio.terra.common.exception.ConflictException;

public class ResourceLockConflict extends ConflictException {
  public ResourceLockConflict(String message) {
    super(message);
  }

  public ResourceLockConflict(String message, Throwable cause) {
    super(message, cause);
  }
}
