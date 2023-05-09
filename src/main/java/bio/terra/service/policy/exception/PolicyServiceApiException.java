package bio.terra.service.policy.exception;

import bio.terra.common.exception.ErrorReportException;
import bio.terra.policy.client.ApiException;
import java.util.Collections;
import org.springframework.http.HttpStatus;

/** Wrapper exception for non-200 responses from calls to Terra Policy Service. */
public class PolicyServiceApiException extends ErrorReportException {

  public PolicyServiceApiException(ApiException ex) {
    super(
        "Error from Policy Service: ",
        ex,
        Collections.singletonList(ex.getResponseBody()),
        HttpStatus.resolve(ex.getCode()));
  }
}
