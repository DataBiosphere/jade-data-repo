package bio.terra.service.common;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;

public interface CreateAzureStorageAccountStep extends DefaultUndoStep {
  ResourceService resourceService();

  Dataset dataset();

  default void getOrCreateDatasetStorageAccount(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    AzureStorageAccountResource storageAccountResource =
        resourceService().getOrCreateDatasetStorageAccount(dataset(), billingProfile, flightId);
    workingMap.put(CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, storageAccountResource);

    AzureStorageAuthInfo storageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(billingProfile, storageAccountResource);
    workingMap.put(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, storageAuthInfo);
  }
}
