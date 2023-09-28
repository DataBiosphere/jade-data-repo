package bio.terra.service.resourcemanagement.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteLogAnalyticsWorkspaceStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteLogAnalyticsWorkspaceStep.class);
  private AzureMonitoringService monitoringService;

  public DeleteLogAnalyticsWorkspaceStep(AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    List<AzureStorageAccountResource> storageAccounts =
        workingMap.get(
            ProfileMapKeys.PROFILE_STORAGE_ACCOUNT_RESOURCE_LIST, new TypeReference<>() {});

    for (AzureStorageAccountResource storageAccountResource : storageAccounts) {
      if (monitoringService.getLogAnalyticsWorkspace(profileModel, storageAccountResource)
          != null) {
        logger.info(
            "Log Analytics Workspace found for storage account {}; Attempting delete.",
            storageAccountResource.getName());
        monitoringService.deleteLogAnalyticsWorkspace(profileModel, storageAccountResource);
      } else {
        logger.warn(
            "Log Analytics Workspace NOT FOUND for storage account {}; Skipping delete.",
            storageAccountResource.getName());
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
