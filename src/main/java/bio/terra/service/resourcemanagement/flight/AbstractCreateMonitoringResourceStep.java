package bio.terra.service.resourcemanagement.flight;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.exception.NotFoundException;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public abstract class AbstractCreateMonitoringResourceStep implements Step {

  protected final AzureMonitoringService monitoringService;
  protected final AzureRegion region;

  public AbstractCreateMonitoringResourceStep(
      AzureMonitoringService monitoringService, AzureRegion region) {
    this.monitoringService = monitoringService;
    this.region = region;
  }

  /**
   * Return the storage account resource object regardless of whether it is a dataset or snapshot
   *
   * <p>Note: always force the region to the one specified in the constructor since it's not always
   * possible to get the region from the storage account resource while the flight is running
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
      if (region != null) {
        storageAccountResource.region(region);
      }
      return storageAccountResource;
    }
    storageAccountResource =
        context
            .getWorkingMap()
            .get(
                CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    if (storageAccountResource != null) {
      if (region != null) {
        storageAccountResource.region(region);
      }
      return storageAccountResource;
    }
    throw new NotFoundException(
        "Storage account resource not found in the current flight's working map");
  }
}
