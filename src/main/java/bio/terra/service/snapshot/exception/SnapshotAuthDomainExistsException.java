package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.ConflictException;

public class SnapshotAuthDomainExistsException extends ConflictException {
  public SnapshotAuthDomainExistsException(String message) {
    super(message);
  }
}
