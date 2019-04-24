package bio.terra.service.exception;

import bio.terra.exception.NotFoundException;

public class DrsObjectNotFoundException extends NotFoundException {
    public DrsObjectNotFoundException(String message) {
        super(message);
    }

    public DrsObjectNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DrsObjectNotFoundException(Throwable cause) {
        super(cause);
    }
}
