package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.NotFoundException;

public class AssetNotFoundException extends NotFoundException {
  public AssetNotFoundException(String message) {
    super(message);
  }

  public AssetNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public AssetNotFoundException(Throwable cause) {
    super(cause);
  }
}
