package bio.terra.service.resourcemanagement.flight;

import bio.terra.common.ErrorCollector;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import java.util.UUID;

public class DeleteSentinelStep extends AbstractDeleteMonitoringResourceStep {
  public DeleteSentinelStep(
      AzureMonitoringService monitoringService,
      UUID subscriptionId,
      String resourceGroupName,
      String storageAccountName,
      ErrorCollector errorCollector) {
    super(monitoringService, subscriptionId, resourceGroupName, storageAccountName, errorCollector);
    resourceName = "Sentinel";
  }

  boolean resourceExists(UUID subscriptionId, String resourceGroupName, String storageAccountName) {
    return monitoringService.getSentinel(subscriptionId, resourceGroupName, storageAccountName)
        != null;
  }

  void deleteResource(UUID subscriptionId, String resourceGroupName, String storageAccountName) {
    monitoringService.deleteSentinel(subscriptionId, resourceGroupName, storageAccountName);
  }
}
