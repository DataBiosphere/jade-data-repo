package bio.terra.common.exception;

import java.util.List;
import java.util.stream.Collectors;

/**
 * DataRepoException is the base exception for the other data repository exceptions.
 * It adds a list of strings to provide error details. These are used in several cases.
 * For example,
 * <ul>
 *     <li>validation errors - to return details of each check that failed</li>
 *     <li>id mismatch errors - invalid file id and row id references</li>
 * </ul>
 */
public class DataRepoException extends RuntimeException {
    private final List<String> errorDetails;

    public DataRepoException(String message) {
        super(message);
        this.errorDetails = null;
    }

    public DataRepoException(String message, Throwable cause) {
        super(message, cause);
        this.errorDetails = null;
    }

    public DataRepoException(Throwable cause) {
        super(cause);
        this.errorDetails = null;
    }

    public DataRepoException(String message, List<String> errorDetails) {
        super(message);
        this.errorDetails = errorDetails;
    }

    public DataRepoException(String message, Throwable cause, List<String> errorDetails) {
        super(message, cause);
        this.errorDetails = errorDetails;
    }

    public List<String> getErrorDetails() {
        return errorDetails;
    }

    @Override
    public String toString() {
        if (errorDetails == null) {
            return super.toString();
        }
        String details = errorDetails.stream().collect(Collectors.joining("; "));
        return super.toString() + " Details: " + details;
    }
}
