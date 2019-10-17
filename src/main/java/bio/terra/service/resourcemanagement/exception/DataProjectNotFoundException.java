package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.NotFoundException;

public class DataProjectNotFoundException extends NotFoundException {
    public DataProjectNotFoundException(String message) {
        super(message);
    }

    public DataProjectNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataProjectNotFoundException(Throwable cause) {
        super(cause);
    }
}
