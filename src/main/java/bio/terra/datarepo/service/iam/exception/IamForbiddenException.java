package bio.terra.datarepo.service.iam.exception;

import bio.terra.datarepo.common.exception.UnauthorizedException;
import java.util.List;

public class IamForbiddenException extends UnauthorizedException {
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
