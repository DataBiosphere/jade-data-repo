package bio.terra.service.resourcemanagement.flight;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.ErrorCollector;
import bio.terra.common.exception.NotFoundException;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import java.util.UUID;

public abstract class AbstractDeleteMonitoringResourceStep extends DefaultUndoStep {

  protected final AzureMonitoringService monitoringService;
  protected UUID subscriptionId = null;
  protected String resourceGroupName = null;
  protected String storageAccountName = null;
  protected ErrorCollector errorCollector = null;

  public AbstractDeleteMonitoringResourceStep(
      AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  void populateVariables(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    subscriptionId = workingMap.get(AzureStorageMapKeys.SUBSCRIPTION_ID, UUID.class);
    resourceGroupName = workingMap.get(AzureStorageMapKeys.RESOURCE_GROUP_NAME, String.class);
    storageAccountName = workingMap.get(AzureStorageMapKeys.STORAGE_ACCOUNT_NAME, String.class);
    errorCollector = workingMap.get(AzureStorageMapKeys.ERROR_COLLECTOR, ErrorCollector.class);
  }


}
