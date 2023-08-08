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

public class CreateSentinelNotificationRuleStep extends AbstractCreateMonitoringResourceStep {

  public CreateSentinelNotificationRuleStep(
      AzureMonitoringService monitoringService, AzureRegion region) {
    super(monitoringService, region);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount = getStorageAccount(context);
    if (monitoringService.getNotificationRule(profileModel, storageAccount) != null) {
      return StepResult.getStepResultSuccess();
    }
    String notificationRuleID =
        monitoringService.createNotificationRule(profileModel, storageAccount);
    // Only record if we created the Sentinel deployment as an indicator that we need to remove
    // it on failure
    workingMap.put(AzureMonitoringMapKeys.NOTIFICATION_RULE_ID, notificationRuleID);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String notificationRuleID =
        workingMap.get(AzureMonitoringMapKeys.NOTIFICATION_RULE_ID, String.class);
    if (notificationRuleID != null) {
      monitoringService.deleteNotificationRule(profileModel, notificationRuleID);
    }
    return StepResult.getStepResultSuccess();
  }
}
