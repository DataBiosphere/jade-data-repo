package bio.terra.stairway.exception;

public class JsonConversionException extends StairwayRuntimeException {
    public JsonConversionException(String message) {
        super(message);
    }

    public JsonConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonConversionException(Throwable cause) {
        super(cause);
    }
}
