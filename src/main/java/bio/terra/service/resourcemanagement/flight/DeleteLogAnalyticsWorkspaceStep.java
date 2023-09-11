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
import java.util.UUID;

public class DeleteLogAnalyticsWorkspaceStep extends AbstractDeleteMonitoringResourceStep {
  public DeleteLogAnalyticsWorkspaceStep(
      AzureMonitoringService monitoringService) {
    super(monitoringService);
    resourceName = "Log Analytics Workspace";
  }

  boolean resourceExists(UUID subscriptionId, String resourceGroupName, String storageAccountName) {
    return monitoringService.getLogAnalyticsWorkspace(subscriptionId, resourceGroupName, storageAccountName) != null;
  }

  void deleteResource(UUID subscriptionId, String resourceGroupName, String storageAccountName) {
    monitoringService.deleteLogAnalyticsWorkspaceByName(subscriptionId, resourceGroupName, storageAccountName);
  }

}
