package bio.terra.service.job.exception;

public class InvalidJobParameterException extends RuntimeException {
    public InvalidJobParameterException(String message) {
        super(message);
    }

    public InvalidJobParameterException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidJobParameterException(Throwable cause) {
        super(cause);
    }
}
