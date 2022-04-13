package bio.terra.service.auth.iam.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class IamInternalServerErrorException extends InternalServerErrorException {
  public IamInternalServerErrorException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamInternalServerErrorException(String message) {
    super(message);
  }

  public IamInternalServerErrorException(Throwable cause) {
    super(cause);
  }

  public IamInternalServerErrorException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
