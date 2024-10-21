package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class SnapshotUpdateException extends InternalServerErrorException {
  public SnapshotUpdateException(String message) {
    super(message);
  }

  public SnapshotUpdateException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotUpdateException(Throwable cause) {
    super(cause);
  }

  public SnapshotUpdateException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
