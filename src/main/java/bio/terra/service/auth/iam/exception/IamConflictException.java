package bio.terra.service.auth.iam.exception;

import bio.terra.common.exception.ConflictException;
import java.util.List;

public class IamConflictException extends ConflictException {
  public IamConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamConflictException(Throwable cause) {
    super(cause);
  }

  public IamConflictException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
