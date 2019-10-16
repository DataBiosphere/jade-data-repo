package bio.terra.flight.exception;

import bio.terra.exception.BadRequestException;

public class InvalidUriException extends BadRequestException {
    public InvalidUriException(String message) {
        super(message);
    }

    public InvalidUriException(String message, Throwable cause) {
        super(message, cause);
    }
}
