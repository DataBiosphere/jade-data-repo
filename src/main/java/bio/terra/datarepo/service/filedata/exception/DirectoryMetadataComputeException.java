package bio.terra.service.filedata.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class DirectoryMetadataComputeException extends InternalServerErrorException {
  public DirectoryMetadataComputeException(String message) {
    super(message);
  }

  public DirectoryMetadataComputeException(String message, Throwable cause) {
    super(message, cause);
  }

  public DirectoryMetadataComputeException(Throwable cause) {
    super(cause);
  }
}
