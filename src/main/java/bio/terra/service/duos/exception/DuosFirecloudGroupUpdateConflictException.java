package bio.terra.service.duos.exception;

import bio.terra.common.exception.ConflictException;
import java.util.List;

public class DuosFirecloudGroupUpdateConflictException extends ConflictException {
  public DuosFirecloudGroupUpdateConflictException(String message) {
    super(message);
  }

  public DuosFirecloudGroupUpdateConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuosFirecloudGroupUpdateConflictException(Throwable cause) {
    super(cause);
  }

  public DuosFirecloudGroupUpdateConflictException(
      String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
