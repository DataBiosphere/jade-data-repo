package bio.terra.datarepo.service.filedata.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class BulkLoadFileMaxExceededException extends BadRequestException {
  public BulkLoadFileMaxExceededException(String message) {
    super(message);
  }

  public BulkLoadFileMaxExceededException(String message, Throwable cause) {
    super(message, cause);
  }

  public BulkLoadFileMaxExceededException(Throwable cause) {
    super(cause);
  }
}
