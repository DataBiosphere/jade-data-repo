package bio.terra.resourcemanagement.service.exception;

import bio.terra.exception.InternalServerErrorException;

public class BillingServiceException extends InternalServerErrorException {
    public BillingServiceException(String message) {
        super(message);
    }

    public BillingServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public BillingServiceException(Throwable cause) {
        super(cause);
    }
}
