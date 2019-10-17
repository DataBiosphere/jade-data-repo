package bio.terra.common.exception;

import java.util.List;

public class BadRequestException extends DataRepoException {
    public BadRequestException(String message) {
        super(message);
    }

    public BadRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadRequestException(Throwable cause) {
        super(cause);
    }

    public BadRequestException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }

    public BadRequestException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
