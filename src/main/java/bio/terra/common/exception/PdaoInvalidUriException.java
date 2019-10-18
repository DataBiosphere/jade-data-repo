package bio.terra.common.exception;

public class PdaoInvalidUriException extends BadRequestException {
    public PdaoInvalidUriException(String message) {
        super(message);
    }

    public PdaoInvalidUriException(String message, Throwable cause) {
        super(message, cause);
    }

    public PdaoInvalidUriException(Throwable cause) {
        super(cause);
    }
}
