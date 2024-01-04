package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class InaccessibleBillingAccountException extends BadRequestException {
  public InaccessibleBillingAccountException(String message) {
    super(message);
  }

  public InaccessibleBillingAccountException(String message, Throwable cause) {
    super(message, cause);
  }

  public InaccessibleBillingAccountException(Throwable cause) {
    super(cause);
  }

  public InaccessibleBillingAccountException(String message, List<String> causes) {
    super(message, causes);
  }
}
