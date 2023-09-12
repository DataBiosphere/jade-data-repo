package bio.terra.service.resourcemanagement.flight;

import bio.terra.common.ErrorCollector;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import java.util.UUID;

public class DeleteLogAnalyticsWorkspaceStep extends AbstractDeleteMonitoringResourceStep {
  public DeleteLogAnalyticsWorkspaceStep(
      AzureMonitoringService monitoringService,
      UUID subscriptionId,
      String resourceGroupName,
      String storageAccountName,
      ErrorCollector errorCollector) {
    super(monitoringService, subscriptionId, resourceGroupName, storageAccountName, errorCollector);
    resourceName = "Log Analytics Workspace";
  }

  boolean resourceExists(UUID subscriptionId, String resourceGroupName, String storageAccountName) {
    return monitoringService.getLogAnalyticsWorkspace(
            subscriptionId, resourceGroupName, storageAccountName)
        != null;
  }

  void deleteResource(UUID subscriptionId, String resourceGroupName, String storageAccountName) {
    monitoringService.deleteLogAnalyticsWorkspaceByName(
        subscriptionId, resourceGroupName, storageAccountName);
  }
}
