package scripts.testscripts;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.client.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.utils.DataRepoUtils;

public class ServiceStatus extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(ServiceStatus.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public ServiceStatus() {
    super();
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(apiClient);
    unauthenticatedApi.serviceStatus();

    int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
    logger.debug("Service status: {}", httpStatus);
  }
}
