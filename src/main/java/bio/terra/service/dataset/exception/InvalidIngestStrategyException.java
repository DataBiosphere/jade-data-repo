package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidIngestStrategyException extends BadRequestException {
    public InvalidIngestStrategyException(String message) {
        super(message);
    }
}
