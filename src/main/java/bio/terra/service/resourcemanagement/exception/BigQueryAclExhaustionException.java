package bio.terra.service.resourcemanagement.exception;

public class BigQueryAclExhaustionException extends GoogleResourceException {
  // This constructor is required so this can be deserialized using StairwayExceptionSerializer.
  public BigQueryAclExhaustionException(String message) {
    super(message);
  }

  public BigQueryAclExhaustionException(String message, Throwable cause) {
    super(message, cause);
  }
}
