package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class EnablePermissionsFailedException extends InternalServerErrorException {
    public EnablePermissionsFailedException(String message) {
        super(message);
    }

    public EnablePermissionsFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public EnablePermissionsFailedException(Throwable cause) {
        super(cause);
    }
}
