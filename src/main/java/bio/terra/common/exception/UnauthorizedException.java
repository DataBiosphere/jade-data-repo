package bio.terra.common.exception;

import java.util.List;

public abstract class UnauthorizedException extends DataRepoException {
    public UnauthorizedException(String message) {
        super(message);
    }

    public UnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnauthorizedException(Throwable cause) {
        super(cause);
    }

    public UnauthorizedException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }

    public UnauthorizedException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
