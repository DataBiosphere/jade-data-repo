package bio.terra.common.exception;

import org.springframework.dao.DataAccessException;


public class RetryQueryException extends DataAccessException {
    public RetryQueryException(String message) {
        super(message);
    }

    public RetryQueryException(String message, Throwable cause) {
        super(message, cause);
    }

}
