package bio.terra.common.exception;

public class ServiceInitializationException extends InternalServerErrorException {

  public ServiceInitializationException(String message) {
    super(message);
  }

  public ServiceInitializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
