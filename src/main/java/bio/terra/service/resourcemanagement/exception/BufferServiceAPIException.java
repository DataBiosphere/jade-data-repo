package bio.terra.service.resourcemanagement.exception;

import bio.terra.buffer.client.ApiException;
import bio.terra.common.exception.ErrorReportException;
import java.util.Collections;
import java.util.List;
import org.springframework.http.HttpStatus;

/** Wrapper exception for non-200 responses from calls to Buffer Service. */
public class BufferServiceAPIException extends ErrorReportException {
    private ApiException apiException;

    public BufferServiceAPIException(ApiException bufferException) {
        super(
                "Error from Buffer Service: ",
                bufferException,
                Collections.singletonList(bufferException.getResponseBody()),
                HttpStatus.resolve(bufferException.getCode()));
        this.apiException = bufferException;
    }

    public BufferServiceAPIException(String message) {
        super(message);
    }

    public BufferServiceAPIException(String message, Throwable cause) {
        super(message, cause);
    }

    public BufferServiceAPIException(Throwable cause) {
        super(cause);
    }

    public BufferServiceAPIException(String message, List<String> causes, HttpStatus statusCode) {
        super(message, causes, statusCode);
    }

    public BufferServiceAPIException(
            String message, Throwable cause, List<String> causes, HttpStatus statusCode) {
        super(message, cause, causes, statusCode);
    }

    /** Get the HTTP status code of the underlying response from Buffer Service. */
    public int getApiExceptionStatus() {
        return apiException.getCode();
    }
}