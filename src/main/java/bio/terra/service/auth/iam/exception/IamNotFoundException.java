package bio.terra.service.auth.iam.exception;

import bio.terra.common.exception.NotFoundException;
import java.util.List;

public class IamNotFoundException extends NotFoundException {
  public IamNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public IamNotFoundException(Throwable cause) {
    super(cause);
  }

  public IamNotFoundException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
