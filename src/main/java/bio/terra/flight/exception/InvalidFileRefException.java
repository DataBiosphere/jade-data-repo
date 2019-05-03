package bio.terra.flight.exception;

import bio.terra.exception.BadRequestException;

import java.util.List;

public class InvalidFileRefException extends BadRequestException {
    public InvalidFileRefException(String message) {
        super(message);
    }

    public InvalidFileRefException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidFileRefException(Throwable cause) {
        super(cause);
    }

    public InvalidFileRefException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }
}
