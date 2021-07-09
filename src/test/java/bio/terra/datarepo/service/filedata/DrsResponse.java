package bio.terra.datarepo.service.filedata;

import bio.terra.datarepo.model.DRSError;
import bio.terra.integration.ObjectOrErrorResponse;
import java.util.Optional;
import org.springframework.http.HttpStatus;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel! Describe the change and
 * any consequences downstream to DRS clients.
 */
/** Specialization of ObjectOrErrorResponse for ErrorModel */
public class DrsResponse<T> {
  private ObjectOrErrorResponse<DRSError, T> response;

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
