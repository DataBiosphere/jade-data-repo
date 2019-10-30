package bio.terra.service.iam.exception;

import bio.terra.common.exception.UnauthorizedException;

public class SamUnauthorizedException extends UnauthorizedException {
    public SamUnauthorizedException(String message) {
        super(message);
    }

    public SamUnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    public SamUnauthorizedException(Throwable cause) {
        super(cause);
    }
}
