package bio.terra.common.exception;

public class PdaoFileCopyException extends BadRequestException {
    public PdaoFileCopyException(String message) {
        super(message);
    }

    public PdaoFileCopyException(String message, Throwable cause) {
        super(message, cause);
    }

    public PdaoFileCopyException(Throwable cause) {
        super(cause);
    }
}
