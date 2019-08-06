package bio.terra.integration;

import bio.terra.model.DRSError;
import org.springframework.http.HttpStatus;

import java.util.Optional;

/**
 * Specialization of ObjectOrErrorResponse for ErrorModel
 */
public class DrsResponse<T> {
    private ObjectOrErrorResponse<DRSError, T>  response;

    public DrsResponse(ObjectOrErrorResponse<DRSError, T> response) {
        this.response = response;
    }

    public HttpStatus getStatusCode() {
        return response.getStatusCode();
    }

    public Optional<DRSError> getErrorObject() {
        return response.getErrorObject();
    }

    public Optional<T> getResponseObject() {
        return response.getResponseObject();
    }
}
