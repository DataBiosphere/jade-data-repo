package bio.terra.service.duos.exception;

import bio.terra.common.exception.InternalServerErrorException;
import java.util.List;

public class DuosInternalServerErrorException extends InternalServerErrorException {
  public DuosInternalServerErrorException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuosInternalServerErrorException(String message) {
    super(message);
  }

  public DuosInternalServerErrorException(Throwable cause) {
    super(cause);
  }

  public DuosInternalServerErrorException(String message, List<String> errorDetails) {
    super(message, errorDetails);
  }
}
