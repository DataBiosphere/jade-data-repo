package bio.terra.datarepo.service.tabulardata.exception;

import bio.terra.datarepo.common.exception.BadRequestException;
import java.util.List;

public class MismatchedRowIdException extends BadRequestException {

  public MismatchedRowIdException(String message) {
    super(message);
  }

  public MismatchedRowIdException(String message, List<String> errors) {
    super(message, errors);
  }

  public MismatchedRowIdException(String message, Throwable cause) {
    super(message, cause);
  }

  public MismatchedRowIdException(String message, Throwable cause, List<String> errors) {
    super(message, cause, errors);
  }

  public MismatchedRowIdException(Throwable cause) {
    super(cause);
  }
}
