package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class InvalidAssetException extends BadRequestException {
  public InvalidAssetException(String message) {
    super(message);
  }

  public InvalidAssetException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidAssetException(Throwable cause) {
    super(cause);
  }

  public InvalidAssetException(String message, List<String> causes) {
    super(message, causes);
  }

  public InvalidAssetException(String message, Throwable cause, List<String> errorDetails) {
    super(message, cause, errorDetails);
  }
}
