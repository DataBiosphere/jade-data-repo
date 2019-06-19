package bio.terra.resourcemanagement.dao.exception;

import bio.terra.exception.BadRequestException;

import java.util.List;

public class AccountAlreadyInUse extends BadRequestException {
    public AccountAlreadyInUse(String message) {
        super(message);
    }

    public AccountAlreadyInUse(String message, Throwable cause) {
        super(message, cause);
    }

    public AccountAlreadyInUse(Throwable cause) {
        super(cause);
    }

    public AccountAlreadyInUse(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause, errorDetails);
    }
}
