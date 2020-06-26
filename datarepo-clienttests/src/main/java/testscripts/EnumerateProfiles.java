package testscripts;

import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;

public class EnumerateProfiles extends runner.TestScript {

    /**
     * Public constructor so that this class can be instantiated via reflection.
     */
    public EnumerateProfiles() {
        super();
    }


    public void userJourney() throws ApiException {
        ResourcesApi resourcesApi = new ResourcesApi();
        EnumerateBillingProfileModel profiles = resourcesApi.enumerateProfiles(0, 10);

        int httpStatus = resourcesApi.getApiClient().getStatusCode();
        System.out.println("Enumerate profiles: " + httpStatus);
    }

}
