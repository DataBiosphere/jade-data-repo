package bio.terra.service.resourcemanagement.flight;

import bio.terra.common.ErrorCollector;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DeleteCloudStorageAccountStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractDeleteMonitoringResourceStep.class);

  protected final AzureStorageAccountService storageAccountService;
  private final UUID subscriptionId;
  private final String resourceGroupName;
  private final String storageAccountName;
  private final ErrorCollector errorCollector;

  public DeleteCloudStorageAccountStep(
      AzureStorageAccountService storageAccountService, UUID subscriptionId, String resourceGroupName, String storageAccountName, ErrorCollector errorCollector) {
    this.storageAccountService = storageAccountService;
    this.subscriptionId = subscriptionId;
    this.resourceGroupName = resourceGroupName;
    this.storageAccountName = storageAccountName;
    this.errorCollector = errorCollector;
  }
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    if (storageAccountService.getCloudStorageAccount(subscriptionId, resourceGroupName, storageAccountName) == null) {
      errorCollector.record("Storage Account not found, skipping deletion");
      return StepResult.getStepResultSuccess();
    }
    try {
      storageAccountService.deleteCloudStorageAccount(subscriptionId, resourceGroupName, storageAccountName);
      logger.info("Storage Account deleted for storage account {}", storageAccountName);
    } catch (Exception e) {
      errorCollector.record(
          String.format(
              "Failed to delete Storage Account %s in resource group %s",
              storageAccountName, resourceGroupName),
          e);
    }
    return StepResult.getStepResultSuccess();
  }


}
