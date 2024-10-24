package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class TransactionCommitException extends BadRequestException {

  public TransactionCommitException(String message, List<String> causes) {
    super(message, causes);
  }
}
