package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class BucketLockException extends BadRequestException {
  public BucketLockException(String message) {
    super(message);
  }

  public BucketLockException(String message, Throwable cause) {
    super(message, cause);
  }

  public BucketLockException(Throwable cause) {
    super(cause);
  }

  public BucketLockException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
