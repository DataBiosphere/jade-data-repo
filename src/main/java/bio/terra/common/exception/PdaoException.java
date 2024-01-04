package bio.terra.common.exception;

import java.util.List;

public class PdaoException extends InternalServerErrorException {
  public PdaoException(String message) {
    super(message);
  }

  public PdaoException(String message, Throwable cause) {
    super(message, cause);
  }

  public PdaoException(Throwable cause) {
    super(cause);
  }

  public PdaoException(String message, List<String> causes) {
    super(message, causes);
  }
}
