package bio.terra.datarepo.service.snapshot.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class SnapshotNotFoundException extends NotFoundException {
  public SnapshotNotFoundException(String message) {
    super(message);
  }

  public SnapshotNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotNotFoundException(Throwable cause) {
    super(cause);
  }
}
