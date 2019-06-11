package bio.terra.dao.exception;

import bio.terra.exception.BadRequestException;

public class AccountAlreadyExistsException extends BadRequestException {
    public AccountAlreadyExistsException(String message) {
        super(message);
    }

    public AccountAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public AccountAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
