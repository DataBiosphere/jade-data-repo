package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.NotFoundException;

public class SnapshotAuthDomainNotFoundException extends NotFoundException {
  public SnapshotAuthDomainNotFoundException(String message) {
    super(message);
  }
}
