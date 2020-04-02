package bio.terra.service.dataset.exception;

import bio.terra.common.exception.InternalServerErrorException;

import java.util.List;

public class InvalidLockUsageException extends InternalServerErrorException {
    public InvalidLockUsageException(String message) {
        super(message);
    }

    public InvalidLockUsageException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidLockUsageException(Throwable cause) {
        super(cause);
    }

    public InvalidLockUsageException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
