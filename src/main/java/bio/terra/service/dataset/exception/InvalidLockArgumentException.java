package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;

import java.util.List;

public class InvalidLockArgumentException extends InternalServerErrorException {
    public InvalidLockArgumentException(String message) {
        super(message);
    }

    public InvalidLockArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidLockArgumentException(Throwable cause) {
        super(cause);
    }

    public InvalidLockArgumentException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
