package bio.terra.service.auth.iam.exception;

import bio.terra.common.exception.ForbiddenException;
import java.util.List;

public class IamForbiddenException extends ForbiddenException {
  public IamForbiddenException(String message) {
    super(message);
  }

  public IamForbiddenException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamForbiddenException(Throwable cause) {
    super(cause);
  }

  public IamForbiddenException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
