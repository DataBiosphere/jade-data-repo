package bio.terra.pdao.exception;

public class PdaoException extends RuntimeException {
    public PdaoException(String message) {
        super(message);
    }

    public PdaoException(String message, Throwable cause) {
        super(message, cause);
    }

    public PdaoException(Throwable cause) {
        super(cause);
    }
}
