package testscripts;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceStatus extends runner.TestScript {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceStatus.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public ServiceStatus() {
    super();
  }

  public void userJourney(ApiClient apiClient) throws ApiException {
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(apiClient);
    unauthenticatedApi.serviceStatus();

    int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
    LOG.info("Service status: {}", httpStatus);
  }
}
