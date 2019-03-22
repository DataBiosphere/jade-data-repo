package bio.terra.flight.exception;

import bio.terra.exception.BadRequestException;

public class IngestFailureException extends BadRequestException {
    public IngestFailureException(String message) {
        super(message);
    }

    public IngestFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public IngestFailureException(Throwable cause) {
        super(cause);
    }
}
