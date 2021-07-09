package bio.terra.datarepo.service.snapshot.exception;

import bio.terra.datarepo.common.exception.InternalServerErrorException;

public class CorruptMetadataException extends InternalServerErrorException {
  public CorruptMetadataException(String message) {
    super(message);
  }

  public CorruptMetadataException(String message, Throwable cause) {
    super(message, cause);
  }

  public CorruptMetadataException(Throwable cause) {
    super(cause);
  }
}
