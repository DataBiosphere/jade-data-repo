package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.BadRequestException;

import java.util.List;

public class StorageAccountLockException extends BadRequestException {
    public StorageAccountLockException(String message) {
        super(message);
    }

    public StorageAccountLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageAccountLockException(Throwable cause) {
        super(cause);
    }

    public StorageAccountLockException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
