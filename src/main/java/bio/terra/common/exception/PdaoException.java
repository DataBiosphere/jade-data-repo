package bio.terra.common.exception;

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
}
