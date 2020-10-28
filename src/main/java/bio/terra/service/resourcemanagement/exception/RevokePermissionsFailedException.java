package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class RevokePermissionsFailedException extends InternalServerErrorException {
    public RevokePermissionsFailedException(String message) {
        super(message);
    }

    public RevokePermissionsFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RevokePermissionsFailedException(Throwable cause) {
        super(cause);
    }
}
