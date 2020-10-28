package bio.terra.common.exception;

import java.util.List;


public class RetryQueryException extends InternalServerErrorException {
    public RetryQueryException(String message) {
        super(message);
    }

    public RetryQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryQueryException(Throwable cause) {
        super(cause);
    }

    public RetryQueryException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }

}
