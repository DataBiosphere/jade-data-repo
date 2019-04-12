package bio.terra.pdao.exception;

import bio.terra.exception.NotFoundException;

public class PdaoSourceFileNotFoundException extends NotFoundException {
    public PdaoSourceFileNotFoundException(String message) {
        super(message);
    }

    public PdaoSourceFileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public PdaoSourceFileNotFoundException(Throwable cause) {
        super(cause);
    }
}
