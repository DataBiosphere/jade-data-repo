package bio.terra.resourcemanagement.dao.google;

import bio.terra.exception.NotFoundException;

public class GoogleResourceNotFoundException extends NotFoundException {
    public GoogleResourceNotFoundException(String message) {
        super(message);
    }

    public GoogleResourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public GoogleResourceNotFoundException(Throwable cause) {
        super(cause);
    }
}
