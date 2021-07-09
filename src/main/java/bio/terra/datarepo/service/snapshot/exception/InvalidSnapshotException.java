package bio.terra.datarepo.service.snapshot.exception;

import bio.terra.datarepo.common.exception.BadRequestException;
import java.util.List;

public class InvalidSnapshotException extends BadRequestException {
  public InvalidSnapshotException(String message) {
    super(message);
  }

  public InvalidSnapshotException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidSnapshotException(Throwable cause) {
    super(cause);
  }

  public InvalidSnapshotException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
