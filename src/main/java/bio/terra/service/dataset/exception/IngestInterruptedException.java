package bio.terra.service.dataset.exception;

import bio.terra.stairway.exception.RetryException;

public class IngestInterruptedException extends RetryException {
    public IngestInterruptedException(String message) {
        super(message);
    }

    public IngestInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }

    public IngestInterruptedException(Throwable cause) {
        super(cause);
    }
}
