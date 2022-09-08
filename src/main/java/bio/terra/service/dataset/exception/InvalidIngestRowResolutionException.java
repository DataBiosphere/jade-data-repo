package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;
import java.util.List;

public class InvalidIngestRowResolutionException extends BadRequestException {
  public InvalidIngestRowResolutionException(String message, List<String> causes) {
    super(message, causes);
  }
}
