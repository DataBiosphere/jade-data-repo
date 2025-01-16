package bio.terra.service.resourcemanagement.exception;

public class BigQueryAclExhaustionException extends GoogleResourceException {
  public BigQueryAclExhaustionException(String message) {
    super(message);
  }

  public BigQueryAclExhaustionException(String message, Throwable cause) {
    super(message, cause);
  }
}
