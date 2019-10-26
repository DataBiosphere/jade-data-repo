package bio.terra.service.filedata.exception;

import bio.terra.common.exception.InternalServerErrorException;

// This exception is inspired by the fix to DR-612. FireStore will throw retryable exceptions
// in some cases. We have seen two: first, it will abort transaction retries in high concurrency cases;
// second, it will mis-report index scan results within a transaction.
//
// This retry exception is used to tell flights to initiate a step retry of the FireStore operation.

public class FileSystemRetryException extends InternalServerErrorException {
    public FileSystemRetryException(String message) {
        super(message);
    }

    public FileSystemRetryException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileSystemRetryException(Throwable cause) {
        super(cause);
    }
}
