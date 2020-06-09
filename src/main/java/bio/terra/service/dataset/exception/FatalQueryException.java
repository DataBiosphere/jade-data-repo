package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;

import java.util.List;

public class FatalQueryException extends InternalServerErrorException {
    public FatalQueryException(String message) {
        super(message);
    }

    public FatalQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalQueryException(Throwable cause) {
        super(cause);
    }

    public FatalQueryException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
