package testscripts;

import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnumerateProfiles extends runner.TestScript {

  private static final Logger LOG = LoggerFactory.getLogger(EnumerateProfiles.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public EnumerateProfiles() {
    super();
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    ResourcesApi resourcesApi = new ResourcesApi(apiClient);
    EnumerateBillingProfileModel profiles = resourcesApi.enumerateProfiles(0, 10);

    int httpStatus = resourcesApi.getApiClient().getStatusCode();
    LOG.info(
        "Enumerate profiles: HTTP status {}, number of profiles found = {}",
        httpStatus,
        profiles.getTotal());
  }
}
