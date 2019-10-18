package bio.terra.common.exception;

import java.util.List;

public class NotImplementedException extends DataRepoException {
    public NotImplementedException(String message) {
        super(message);
    }

    public NotImplementedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotImplementedException(Throwable cause) {
        super(cause);
    }

    public NotImplementedException(String message, List<String> errorDetails) {
        super(message, errorDetails);
    }

    public NotImplementedException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }

}
