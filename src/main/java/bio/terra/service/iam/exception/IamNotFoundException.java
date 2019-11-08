package bio.terra.service.iam.exception;

import bio.terra.common.exception.NotFoundException;

public class IamNotFoundException extends NotFoundException {
    public IamNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public IamNotFoundException(Throwable cause) {
        super(cause);
    }
}
