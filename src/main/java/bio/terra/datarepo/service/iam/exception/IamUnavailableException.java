package bio.terra.datarepo.service.iam.exception;

import bio.terra.datarepo.common.exception.ServiceUnavailableException;
import java.util.List;

public class IamUnavailableException extends ServiceUnavailableException {
  public IamUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamUnavailableException(String message) {
    super(message);
  }

  public IamUnavailableException(Throwable cause) {
    super(cause);
  }

  public IamUnavailableException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
