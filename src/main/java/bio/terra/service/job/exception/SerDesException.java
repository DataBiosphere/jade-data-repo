package bio.terra.service.job.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class SerDesException extends InternalServerErrorException {
    public SerDesException(String message) {
        super(message);
    }

    public SerDesException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerDesException(Throwable cause) {
        super(cause);
    }
}
