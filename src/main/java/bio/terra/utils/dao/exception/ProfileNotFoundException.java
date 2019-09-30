package bio.terra.utils.dao.exception;

import bio.terra.exception.NotFoundException;

public class ProfileNotFoundException extends NotFoundException {
    public ProfileNotFoundException(String message) {
        super(message);
    }

    public ProfileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProfileNotFoundException(Throwable cause) {
        super(cause);
    }
}
