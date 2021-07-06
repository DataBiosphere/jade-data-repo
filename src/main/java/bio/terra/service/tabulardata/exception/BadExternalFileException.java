package bio.terra.service.tabulardata.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class BadExternalFileException extends BadRequestException {

  public BadExternalFileException(String message) {
    super(message);
  }

  public BadExternalFileException(String message, List<String> errors) {
    super(message, errors);
  }

  public BadExternalFileException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadExternalFileException(String message, Throwable cause, List<String> errors) {
    super(message, cause, errors);
  }

  public BadExternalFileException(Throwable cause) {
    super(cause);
  }
}
