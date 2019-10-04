package bio.terra.service.job.exception;

public class JobResponseException extends RuntimeException {
    public JobResponseException(String message) {
        super(message);
    }

    public JobResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobResponseException(Throwable cause) {
        super(cause);
    }
}
