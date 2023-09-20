package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class DatasetLockException extends InternalServerErrorException {
  public DatasetLockException(String message) {
    super(message);
  }

  public DatasetLockException(String message, List<String> causes) {
    super(message, causes);
  }

  public DatasetLockException(String message, Throwable cause) {
    super(message, cause);
  }
}
