package bio.terra.stairway.exception;

public class StairwayUnauthorizedException extends StairwayException {
    public StairwayUnauthorizedException(String message) {
        super(message);
    }

    public StairwayUnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    public StairwayUnauthorizedException(Throwable cause) {
        super(cause);
    }

}
