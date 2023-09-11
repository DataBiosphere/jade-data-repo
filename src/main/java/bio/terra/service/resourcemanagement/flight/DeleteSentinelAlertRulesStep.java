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
import com.azure.resourcemanager.securityinsights.SecurityInsightsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSentinelAlertRulesStep extends AbstractDeleteMonitoringResourceStep {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSentinelAlertRulesStep.class);
  public DeleteSentinelAlertRulesStep(
      AzureMonitoringService monitoringService) {
    super(monitoringService);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    populateVariables(context);
    if (monitoringService.getSentinelRuleUnauthorizedAccess(subscriptionId, resourceGroupName, storageAccountName) == null) {
      logger.info("Sentinel rule unauthorized access not found, skipping deletion");
      return StepResult.getStepResultSuccess();
    }
    try {
      monitoringService.deleteSentinelRuleUnauthorizedAccess(subscriptionId, resourceGroupName, storageAccountName);
      logger.info(
          "Successfully deleted Sentinel Rule Unauthorized Access for storage account {}", storageAccountName);
    } catch (Exception e) {
      errorCollector.record(
          String.format(
              "Failed to delete Sentinel notification for storage account %s in resource group %s",
              storageAccountName, resourceGroupName),
          e);
    }
    return StepResult.getStepResultSuccess();
  }
}
