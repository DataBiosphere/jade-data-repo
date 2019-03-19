package bio.terra.integration;

import bio.terra.model.ErrorModel;
import org.springframework.http.HttpStatus;

import java.util.Optional;

/**
 * This class is returned by the data repo client.
 * The idea is to make it simple for callers to assert the properties of the response.
 * The object contains:
 * <ul>
 *     <li>status code from the response</li>
 *     <li>if the status code is success, then it will contain the deserialized object of class T</li>
 *     <li>if the status code is failure, then it will contain the deserialized object of ErrorModel</li>
 * </ul>
 * Errors deserializing the response are thrown from the data repo client.
 */
public class DataRepoResponse<T> {
    private HttpStatus statusCode;
    private Optional<ErrorModel> errorModel;
    private Optional<T> responseObject;

    public HttpStatus getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(HttpStatus statusCode) {
        this.statusCode = statusCode;
    }

    public Optional<ErrorModel> getErrorModel() {
        return errorModel;
    }

    public void setErrorModel(Optional<ErrorModel> errorModel) {
        this.errorModel = errorModel;
    }

    public Optional<T> getResponseObject() {
        return responseObject;
    }

    public void setResponseObject(Optional<T> responseObject) {
        this.responseObject = responseObject;
    }
}
