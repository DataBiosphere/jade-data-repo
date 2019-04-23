package bio.terra.pdao.exception;

import bio.terra.exception.BadRequestException;

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
