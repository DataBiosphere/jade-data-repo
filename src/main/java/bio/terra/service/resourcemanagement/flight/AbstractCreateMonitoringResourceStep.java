package bio.terra.service.resourcemanagement.flight;

import bio.terra.common.exception.NotFoundException;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public abstract class AbstractCreateMonitoringResourceStep implements Step {

  /**
   * Return the storage account resource object regardless of whether it is a dataset or snapshot
   *
   * @param context The current flight's context
   * @return A {@link AzureStorageAccountResource} object or null if not found
   * @throws NotFoundException If a storage account resource is not found
   */
  protected AzureStorageAccountResource getStorageAccount(FlightContext context)
      throws NotFoundException {
    AzureStorageAccountResource storageAccountResource =
        context
            .getWorkingMap()
            .get(CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    if (storageAccountResource != null) {
      return storageAccountResource;
    }
    storageAccountResource =
        context
            .getWorkingMap()
            .get(
                CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    if (storageAccountResource != null) {
      return storageAccountResource;
    }
    throw new NotFoundException(
        "Storage account resource not found in the current flight's working map");
  }
}
