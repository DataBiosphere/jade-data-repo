package bio.terra.flight.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidIngestStrategyException extends BadRequestException {
    public InvalidIngestStrategyException(String message) {
        super(message);
    }
}
