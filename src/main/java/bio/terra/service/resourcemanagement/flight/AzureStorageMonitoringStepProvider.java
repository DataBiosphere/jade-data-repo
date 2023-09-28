package bio.terra.service.resourcemanagement.flight;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.stairway.RetryRuleNone.getRetryRuleNone;

import bio.terra.app.model.AzureRegion;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import bio.terra.stairway.Step;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides methods to generate the steps needed to configure storage account logging and
 * monitoring.
 */
public class AzureStorageMonitoringStepProvider {

  private final AzureMonitoringService monitoringService;

  public AzureStorageMonitoringStepProvider(AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  /**
   * Configure the steps needed to configure storage account logging and monitoring. If the dataset
   * or snapshot has secure monitoring enabled, this will configure extra log retention and
   * monitoring resources
   *
   * @param isSecureMonitoringEnabled Is this a dataset or snapshot with secure monitoring enabled?
   * @param region The Azure region where all monitoring resources should be deployed
   */
  public List<StepDef> configureSteps(boolean isSecureMonitoringEnabled, AzureRegion region) {
    List<StepDef> steps = new ArrayList<>();
    RetryRuleExponentialBackoff expBackoffRetry = getDefaultExponentialBackoffRetryRule();

    // Deploy a Log Analytics Workspace if it doesn't exist already
    steps.add(new StepDef(new CreateLogAnalyticsWorkspaceStep(monitoringService, region)));
    // Create a diagnostic setting for the storage account if it doesn't exist already
    // This is what tells the storage account to send logs to the Log Analytics Workspace
    steps.add(new StepDef(new CreateDiagnosticSettingStep(monitoringService, region)));

    // The following steps are only required for FedRAMP compliance
    // They are not turned on in all cases due to cost
    if (isSecureMonitoringEnabled) {
      // Create a rule to send the Log Analytics logs to a central storage account where the logs
      // will be retained for longer than the default 90 day minimum that the Log Analytics
      // Workspace stores
      steps.add(new StepDef(new CreateExportRuleStep(monitoringService, region)));
      // Deploy a Sentinel Workspace if it doesn't exist already
      steps.add(new StepDef(new CreateSentinelStep(monitoringService, region)));
      // Add any rules to Sentinel that will detect events of interest.  It takes a little bit for
      // Sentinel to be available so adding a retry rule here.
      steps.add(
          new StepDef(
              new CreateSentinelAlertRulesStep(monitoringService, region), expBackoffRetry));
      // Add a notification playbook rule to the Sentinel instance.  This ensures that a Slack
      // notification is sent when an alert is triggered.
      steps.add(new StepDef(new CreateSentinelNotificationRuleStep(monitoringService, region)));
    }
    return steps;
  }

  public List<StepDef> configureDeleteSteps(AzureStorageAccountService azureStorageAccountService) {
    return List.of(
        new StepDef(new DeleteSentinelStep(monitoringService, azureStorageAccountService)),
        new StepDef(new DeleteLogAnalyticsWorkspaceStep(monitoringService)));
  }

  public record StepDef(Step step, RetryRule retryRule) {
    public StepDef(Step step) {
      this(step, getRetryRuleNone());
    }
  }
}
