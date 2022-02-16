package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class TransactionLockException extends BadRequestException {

  public TransactionLockException(String message, List<String> causes) {
    super(message, causes);
  }

  public TransactionLockException(String message, Throwable cause) {
    super(message, cause);
  }
}
