package bio.terra.integration;

import bio.terra.model.ErrorModel;
import org.springframework.http.HttpStatus;

import java.util.Optional;

/**
 * Specialization of ObjectOrErrorResponse for ErrorModel
 */
public class DataRepoResponse<T> {
    private ObjectOrErrorResponse<ErrorModel, T>  response;

    public DataRepoResponse(ObjectOrErrorResponse<ErrorModel, T> response) {
        this.response = response;
    }

    public HttpStatus getStatusCode() {
        return response.getStatusCode();
    }

    public Optional<ErrorModel> getErrorObject() {
        return response.getErrorObject();
    }

    public Optional<T> getResponseObject() {
        return response.getResponseObject();
    }

    public Optional<String> getLocationHeader() {
        return response.getLocationHeader();
    }
}
