package bio.terra.service.upgrade.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidCustomNameException extends BadRequestException {
    public InvalidCustomNameException(String message) {
        super(message);
    }

    public InvalidCustomNameException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidCustomNameException(Throwable cause) {
        super(cause);
    }
}
