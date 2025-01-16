package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.BadRequestException;

public class BigQueryAclExhaustionException extends BadRequestException {
  // This constructor is required so this can be deserialized using StairwayExceptionSerializer.
  public BigQueryAclExhaustionException(String message) {
    super(message);
  }

  public BigQueryAclExhaustionException(String message, Throwable cause) {
    super(message, cause);
  }
}
