package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.NotFoundException;

public class AuthDomainGroupNotFoundException extends NotFoundException {
  public AuthDomainGroupNotFoundException(String message) {
    super(message);
  }
}
