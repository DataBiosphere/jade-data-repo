package bio.terra.service.resourcemanagement.flight;

import bio.terra.app.model.AzureRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSentinelStep extends AbstractDeleteMonitoringResourceStep {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSentinelStep.class);
  public DeleteSentinelStep(AzureMonitoringService monitoringService) {
    super(monitoringService);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    populateVariables(context);
    if (monitoringService.getSentinel(subscriptionId, resourceGroupName, storageAccountName) == null) {
      logger.info("Sentinel not found, skipping deletion");
      return StepResult.getStepResultSuccess();
    }
    try {
      monitoringService.deleteSentinel(subscriptionId, resourceGroupName, storageAccountName);
      logger.info("Sentinel instance deleted for storage account {}", storageAccountName);
    } catch (Exception e) {
      errorCollector.record(
          String.format(
              "Failed to delete Sentinel instance for storage account %s in resource group %s",
              storageAccountName, resourceGroupName),
          e);
    }
    return StepResult.getStepResultSuccess();
  }
}
