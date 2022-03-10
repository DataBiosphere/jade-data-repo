package bio.terra.integration;

import bio.terra.model.ErrorModel;
import java.util.Optional;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.springframework.http.HttpStatus;

/** Specialization of ObjectOrErrorResponse for ErrorModel */
public class DataRepoResponse<T> {
  private ObjectOrErrorResponse<ErrorModel, T> response;

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

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("response", response).toString();
  }
}
