package bio.terra.service.iam.exception;

import bio.terra.common.exception.ConflictException;

public class IamConflictException extends ConflictException {
    public IamConflictException(String message, Throwable cause) {
        super(message, cause);
    }

    public IamConflictException(Throwable cause) {
        super(cause);
    }
}
