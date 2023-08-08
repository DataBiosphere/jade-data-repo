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

public class CreateSentinelAlertRulesStep extends AbstractCreateMonitoringResourceStep {

  public CreateSentinelAlertRulesStep(
      AzureMonitoringService monitoringService, AzureRegion region) {
    super(monitoringService, region);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount = getStorageAccount(context);
    if (monitoringService.getSentinelRuleUnauthorizedAccess(profileModel, storageAccount) != null) {
      return StepResult.getStepResultSuccess();
    }
    String alertRuleUnauthedAccessId =
        monitoringService.createSentinelRuleUnauthorizedAccess(profileModel, storageAccount);
    // Only record if we created the alert rule as an indicator that we need to remove
    // it on failure
    workingMap.put(AzureMonitoringMapKeys.ALERT_RULE_UNAUTHED_ACCESS_ID, alertRuleUnauthedAccessId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String sentinelId =
        workingMap.get(AzureMonitoringMapKeys.ALERT_RULE_UNAUTHED_ACCESS_ID, String.class);
    if (sentinelId != null) {
      AzureStorageAccountResource storageAccount = getStorageAccount(context);
      monitoringService.deleteSentinelRuleUnauthorizedAccess(profileModel, storageAccount);
    }
    return StepResult.getStepResultSuccess();
  }
}
