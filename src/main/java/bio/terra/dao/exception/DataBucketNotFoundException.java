package bio.terra.dao.exception;

import bio.terra.exception.NotFoundException;

public class DataBucketNotFoundException extends NotFoundException {
    public DataBucketNotFoundException(String message) {
        super(message);
    }

    public DataBucketNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBucketNotFoundException(Throwable cause) {
        super(cause);
    }
}
