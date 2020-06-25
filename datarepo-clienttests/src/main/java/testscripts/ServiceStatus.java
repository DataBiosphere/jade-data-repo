package testscripts;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.client.ApiException;

public class ServiceStatus extends runner.TestScript {

    /**
     * Public constructor so that this class can be instantiated via reflection.
     */
    public ServiceStatus() {
        super();
    }


    public void userJourney() throws ApiException {
        UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi();
        unauthenticatedApi.serviceStatus();

        int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
        System.out.println("Service status: " + httpStatus);
    }

}
