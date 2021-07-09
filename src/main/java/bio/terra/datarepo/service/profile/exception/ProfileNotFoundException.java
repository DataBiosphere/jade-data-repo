package bio.terra.datarepo.service.profile.exception;

import bio.terra.datarepo.common.exception.NotFoundException;

public class ProfileNotFoundException extends NotFoundException {
  public ProfileNotFoundException(String message) {
    super(message);
  }

  public ProfileNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProfileNotFoundException(Throwable cause) {
    super(cause);
  }
}
