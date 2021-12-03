package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class SnapshotPreviewException extends InternalServerErrorException {
  public SnapshotPreviewException(String message) {
    super(message);
  }

  public SnapshotPreviewException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotPreviewException(Throwable cause) {
    super(cause);
  }
}
