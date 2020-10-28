package bio.terra.service.iam.exception;

import bio.terra.common.exception.ServiceUnavailableException;

public class IamUnavailableException extends ServiceUnavailableException {
    public IamUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
    public IamUnavailableException(String message) {
        super(message);
    }

    public IamUnavailableException(Throwable cause) {
        super(cause);
    }
}
