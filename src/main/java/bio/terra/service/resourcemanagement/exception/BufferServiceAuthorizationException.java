package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.UnauthorizedException;

import java.util.List;

public class BufferServiceAuthorizationException extends UnauthorizedException {

    public BufferServiceAuthorizationException(String message) {
        super(message);
    }

    public BufferServiceAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    public BufferServiceAuthorizationException(Throwable cause) {
        super(cause);
    }

    public BufferServiceAuthorizationException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }

    public BufferServiceAuthorizationException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
