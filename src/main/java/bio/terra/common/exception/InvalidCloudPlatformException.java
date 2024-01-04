package bio.terra.common.exception;

public class InvalidCloudPlatformException extends ApiException {

  public InvalidCloudPlatformException() {
    this("Unrecognized cloud platform");
  }

  public InvalidCloudPlatformException(String message) {
    super(message);
  }

  public InvalidCloudPlatformException(String message, Throwable cause) {
    super(message, cause);
  }
}
