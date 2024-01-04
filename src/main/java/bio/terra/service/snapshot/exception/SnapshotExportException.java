package bio.terra.service.snapshot.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class SnapshotExportException extends BadRequestException {
  public SnapshotExportException(String message, List<String> causes) {
    super(message, causes);
  }
}
