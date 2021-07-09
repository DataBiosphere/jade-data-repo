package bio.terra.datarepo.service.dataset.exception;

import bio.terra.datarepo.common.exception.BadRequestException;
import java.util.List;

public class InvalidDatasetException extends BadRequestException {
  public InvalidDatasetException(String message) {
    super(message);
  }

  public InvalidDatasetException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidDatasetException(Throwable cause) {
    super(cause);
  }

  public InvalidDatasetException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
