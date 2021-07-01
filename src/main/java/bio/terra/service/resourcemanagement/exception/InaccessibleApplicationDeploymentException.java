package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.BadRequestException;

public class InaccessibleApplicationDeploymentException extends BadRequestException {
  public InaccessibleApplicationDeploymentException(String message) {
    super(message);
  }

  public InaccessibleApplicationDeploymentException(String message, Throwable cause) {
    super(message, cause);
  }

  public InaccessibleApplicationDeploymentException(Throwable cause) {
    super(cause);
  }
}
