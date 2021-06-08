package bio.terra.service.resourcemanagement.exception;

import bio.terra.buffer.client.ApiException;
import bio.terra.common.exception.ErrorReportException;
import java.util.Collections;
import java.util.List;
import org.springframework.http.HttpStatus;

/** Wrapper exception for non-200 responses from calls to Buffer Service. */
public class BufferServiceAPIException extends ErrorReportException {
    private final ApiException apiException;

    public BufferServiceAPIException(ApiException bufferException) {
        super(
                "Error from Buffer Service: ",
                bufferException,
                Collections.singletonList(bufferException.getResponseBody()),
                HttpStatus.resolve(bufferException.getCode()));
        this.apiException = bufferException;
    }

    public BufferServiceAPIException(String message, ApiException apiException) {
        super(message);
        this.apiException = apiException;
    }

    public BufferServiceAPIException(String message, Throwable cause, ApiException apiException) {
        super(message, cause);
        this.apiException = apiException;
    }

    public BufferServiceAPIException(Throwable cause, ApiException apiException) {
        super(cause);
        this.apiException = apiException;
    }

    public BufferServiceAPIException(String message, List<String> causes, HttpStatus statusCode, ApiException apiException) {
        super(message, causes, statusCode);
        this.apiException = apiException;
    }

    public BufferServiceAPIException(
            String message, Throwable cause, List<String> causes, HttpStatus statusCode, ApiException apiException) {
        super(message, cause, causes, statusCode);
        this.apiException = apiException;
    }

    /** Get the HTTP status code of the underlying response from Buffer Service. */
    public int getApiExceptionStatus() {
        return apiException.getCode();
    }
}
