package testscripts;

import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;

public class EnumerateProfiles extends runner.TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public EnumerateProfiles() {
    super();
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    ResourcesApi resourcesApi = new ResourcesApi(apiClient);
    EnumerateBillingProfileModel profiles = resourcesApi.enumerateProfiles(0, 10);

    int httpStatus = resourcesApi.getApiClient().getStatusCode();
    System.out.println("Enumerate profiles: HTTP" + httpStatus + ", TOTAL" + profiles.getTotal());
  }
}
