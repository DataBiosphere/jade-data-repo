package bio.terra.service.iam.exception;

import bio.terra.common.exception.UnauthorizedException;

public class IamUnauthorizedException extends UnauthorizedException {
    public IamUnauthorizedException(String message) {
        super(message);
    }

    public IamUnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    public IamUnauthorizedException(Throwable cause) {
        super(cause);
    }
}
