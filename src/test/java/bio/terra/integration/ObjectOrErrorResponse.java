package bio.terra.integration;

import java.util.Optional;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.springframework.http.HttpStatus;

/**
 * This class is returned by the data repo client. The idea is to make it simple for callers to
 * assert the properties of the response. The object contains:
 *
 * <ul>
 *   <li>status code from the response
 *   <li>location header, if any, from the response
 *   <li>if the status code is success, then it will contain the deserialized object of class T
 *   <li>if the status code is failure, then it will contain the deserialized object of class S
 * </ul>
 *
 * Errors deserializing the response are thrown from the data repo client.
 */
public class ObjectOrErrorResponse<S, T> {
  private HttpStatus statusCode;
  private Optional<String> locationHeader;
  private Optional<S> errorObject;
  private Optional<T> responseObject;

  public HttpStatus getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(HttpStatus statusCode) {
    this.statusCode = statusCode;
  }

  public Optional<S> getErrorObject() {
    return errorObject;
  }

  public void setErrorModel(Optional<S> errorObject) {
    this.errorObject = errorObject;
  }

  public Optional<T> getResponseObject() {
    return responseObject;
  }

  public void setResponseObject(Optional<T> responseObject) {
    this.responseObject = responseObject;
  }

  public Optional<String> getLocationHeader() {
    return locationHeader;
  }

  public void setLocationHeader(Optional<String> locationHeader) {
    this.locationHeader = locationHeader;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("statusCode", statusCode)
        .append("locationHeader", locationHeader)
        .append("errorObject", errorObject)
        .append("responseObject", responseObject)
        .toString();
  }
}
