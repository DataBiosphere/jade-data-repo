package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class UpdatePermissionsFailedException extends InternalServerErrorException {
  public UpdatePermissionsFailedException(String message) {
    super(message);
  }

  public UpdatePermissionsFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public UpdatePermissionsFailedException(Throwable cause) {
    super(cause);
  }
}
