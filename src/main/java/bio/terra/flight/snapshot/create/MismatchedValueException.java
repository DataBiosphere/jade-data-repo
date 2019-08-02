package bio.terra.flight.snapshot.create;

import bio.terra.exception.BadRequestException;

public class MismatchedValueException extends BadRequestException {
    public MismatchedValueException(String message) {
        super(message);
    }

    public MismatchedValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public MismatchedValueException(Throwable cause) {
        super(cause);
    }
}
