package bio.terra.service.resourcemanagement.flight;

import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleNone;
import bio.terra.stairway.Step;
import java.util.ArrayList;
import java.util.List;

public class AzureStorageMonitoringProvider {

  private final AzureMonitoringService monitoringService;

  public AzureStorageMonitoringProvider(AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  /**
   * Configure the steps needed to configure storage account logging and monitoring. If the dataset
   * or snapshot has secure monitoring enabled, this will configure extra log retention and
   * monitoring resources
   *
   * @param isSecureMonitoringEnabled Is this a dataset or snapshot with secure monitoring enabled?
   */
  public List<StepDef> configureSteps(boolean isSecureMonitoringEnabled) {
    List<StepDef> steps = new ArrayList<>();
    RetryRuleNone noRetry = RetryRuleNone.getRetryRuleNone();

    // Deploy a Log Analytics Workspace if it doesn't exist already
    steps.add(new StepDef(new CreateLogAnalyticsWorkspaceStep(monitoringService), noRetry));
    // Create a diagnostic setting for the storage account if it doesn't exist already
    // This is what tells the storage account to send logs to the Log Analytics Workspace
    steps.add(new StepDef(new CreateDiagnosticSettingStep(monitoringService), noRetry));

    // The following steps are only required for FedRAMP compliance
    // They are not turned on in all cases due to cost
    if (isSecureMonitoringEnabled) {
      // Create a rule to send the Log Analytics logs to a central storage account where the logs
      // will be retained for longer than the default 90 day minimum that the Log Analytics
      // Workspace stores
      steps.add(new StepDef(new CreateExportRuleStep(monitoringService), noRetry));
      // Deploy a Sentinel Workspace if it doesn't exist already
      steps.add(new StepDef(new CreateSentinelStep(monitoringService), noRetry));
      // Add any rules to Sentinel that will detect events of interest
      steps.add(new StepDef(new CreateSentinelAlertRulesStep(monitoringService), noRetry));
      // Add a notification playbook rule to the Sentinel instance.  This ensures that a Slack
      // notification is sent when an alert is triggered.
      steps.add(new StepDef(new CreateSentinelNotificationRuleStep(monitoringService), noRetry));
    }
    return steps;
  }

  public record StepDef(Step step, RetryRule retryRule) {}
}
