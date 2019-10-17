package bio.terra.common.exception;

import java.util.List;

public class NotFoundException extends DataRepoException {
    public NotFoundException(String message) {
        super(message);
    }

    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotFoundException(Throwable cause) {
        super(cause);
    }

    public NotFoundException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }

    public NotFoundException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
