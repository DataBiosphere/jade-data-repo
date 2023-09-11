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
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public abstract class AbstractDeleteMonitoringResourceStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractDeleteMonitoringResourceStep.class);

  protected final AzureMonitoringService monitoringService;

  public AbstractDeleteMonitoringResourceStep(
      AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  String resourceName;

  abstract boolean resourceExists(UUID subscriptionId, String resourceGroupName, String storageAccountName);

  abstract void deleteResource(UUID subscriptionId, String resourceGroupName, String storageAccountName);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    UUID subscriptionId = workingMap.get(AzureStorageMapKeys.SUBSCRIPTION_ID, UUID.class);
    String resourceGroupName = workingMap.get(AzureStorageMapKeys.RESOURCE_GROUP_NAME, String.class);
    String storageAccountName = workingMap.get(AzureStorageMapKeys.STORAGE_ACCOUNT_NAME, String.class);
    ErrorCollector errorCollector = workingMap.get(AzureStorageMapKeys.ERROR_COLLECTOR, ErrorCollector.class);
    if (resourceExists(subscriptionId, resourceGroupName, storageAccountName)) {
      logger.info("{} not found, skipping deletion", resourceName);
      return StepResult.getStepResultSuccess();
    }
    try {
      deleteResource(subscriptionId, resourceGroupName, storageAccountName);
      logger.info("{} deleted for storage account {}", resourceName, storageAccountName);
    } catch (Exception e) {
      errorCollector.record(
          String.format(
              "Failed to delete {} for storage account %s in resource group %s",
              resourceName, storageAccountName, resourceGroupName),
          e);
    }
    return StepResult.getStepResultSuccess();
  }


}
