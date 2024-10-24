package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class BucketLockFailureException extends InternalServerErrorException {
  public BucketLockFailureException(String message) {
    super(message);
  }

  public BucketLockFailureException(String message, Throwable cause) {
    super(message, cause);
  }

  public BucketLockFailureException(Throwable cause) {
    super(cause);
  }
}
