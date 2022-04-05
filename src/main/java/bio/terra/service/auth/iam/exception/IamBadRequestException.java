package bio.terra.service.auth.iam.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class IamBadRequestException extends BadRequestException {
  public IamBadRequestException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamBadRequestException(Throwable cause) {
    super(cause);
  }

  public IamBadRequestException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
