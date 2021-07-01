package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class DatasetLockException extends InternalServerErrorException {
  public DatasetLockException(String message) {
    super(message);
  }

  public DatasetLockException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatasetLockException(Throwable cause) {
    super(cause);
  }

  public DatasetLockException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
