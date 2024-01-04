package bio.terra.service.auth.iam.exception;

import bio.terra.common.exception.UnauthorizedException;
import java.util.List;

public class IamUnauthorizedException extends UnauthorizedException {
  public IamUnauthorizedException(String message) {
    super(message);
  }

  public IamUnauthorizedException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamUnauthorizedException(Throwable cause) {
    super(cause);
  }

  public IamUnauthorizedException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
