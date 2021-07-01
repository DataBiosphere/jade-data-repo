package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class SnapshotLockException extends InternalServerErrorException {
  public SnapshotLockException(String message) {
    super(message);
  }

  public SnapshotLockException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotLockException(Throwable cause) {
    super(cause);
  }

  public SnapshotLockException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
