package bio.terra.service.iam.exception;

import bio.terra.common.exception.NotFoundException;

public class SamNotFoundException extends NotFoundException {
    public SamNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public SamNotFoundException(Throwable cause) {
        super(cause);
    }
}
