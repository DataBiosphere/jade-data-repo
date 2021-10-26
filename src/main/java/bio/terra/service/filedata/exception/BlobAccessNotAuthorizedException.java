package bio.terra.service.filedata.exception;

import bio.terra.common.exception.UnauthorizedException;

public class BlobAccessNotAuthorizedException extends UnauthorizedException {
  public BlobAccessNotAuthorizedException(String message) {
    super(message);
  }

  public BlobAccessNotAuthorizedException(String message, Throwable cause) {
    super(message, cause);
  }

  public BlobAccessNotAuthorizedException(Throwable cause) {
    super(cause);
  }
}
