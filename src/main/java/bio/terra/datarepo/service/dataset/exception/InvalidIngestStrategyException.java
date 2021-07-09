package bio.terra.datarepo.service.dataset.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class InvalidIngestStrategyException extends BadRequestException {
  public InvalidIngestStrategyException(String message) {
    super(message);
  }
}
