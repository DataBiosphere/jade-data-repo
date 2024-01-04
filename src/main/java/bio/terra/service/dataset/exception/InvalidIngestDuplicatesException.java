package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class InvalidIngestDuplicatesException extends BadRequestException {
  public InvalidIngestDuplicatesException(String message, List<String> causes) {
    super(message, causes);
  }
}
