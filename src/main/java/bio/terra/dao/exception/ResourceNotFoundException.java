package bio.terra.dao.exception;

import bio.terra.exception.NotFoundException;

public class ResourceNotFoundException extends NotFoundException {
    public ResourceNotFoundException(String message) {
        super(message);
    }

    public ResourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceNotFoundException(Throwable cause) {
        super(cause);
    }
}
