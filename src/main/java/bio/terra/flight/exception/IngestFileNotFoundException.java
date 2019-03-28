package bio.terra.flight.exception;

import bio.terra.exception.NotFoundException;

public class IngestFileNotFoundException extends NotFoundException {
    public IngestFileNotFoundException(String message) {
        super(message);
    }

    public IngestFileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public IngestFileNotFoundException(Throwable cause) {
        super(cause);
    }
}
