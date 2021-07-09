package bio.terra.datarepo.service.profile.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class ProfileInUseException extends BadRequestException {
  public ProfileInUseException(String message) {
    super(message);
  }

  public ProfileInUseException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProfileInUseException(Throwable cause) {
    super(cause);
  }
}
