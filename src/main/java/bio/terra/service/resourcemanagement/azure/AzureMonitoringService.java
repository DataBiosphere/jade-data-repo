package bio.terra.service.resourcemanagement.azure;

import static bio.terra.service.filedata.azure.util.AzureConstants.BAD_REQUEST_CODE;
import static bio.terra.service.filedata.azure.util.AzureConstants.NOT_FOUND_CODE;
import static bio.terra.service.filedata.azure.util.AzureConstants.RESOURCE_NOT_FOUND_CODE;

import bio.terra.model.BillingProfileModel;
import com.azure.core.management.Region;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.loganalytics.LogAnalyticsManager;
import com.azure.resourcemanager.loganalytics.models.DataExport;
import com.azure.resourcemanager.loganalytics.models.Workspace;
import com.azure.resourcemanager.loganalytics.models.WorkspaceSku;
import com.azure.resourcemanager.loganalytics.models.WorkspaceSkuNameEnum;
import com.azure.resourcemanager.monitor.models.DiagnosticSetting;
import com.azure.resourcemanager.monitor.models.DiagnosticSettingsCategory;
import com.azure.resourcemanager.securityinsights.SecurityInsightsManager;
import com.azure.resourcemanager.securityinsights.models.AlertRule;
import com.azure.resourcemanager.securityinsights.models.AlertSeverity;
import com.azure.resourcemanager.securityinsights.models.AutomationRule;
import com.azure.resourcemanager.securityinsights.models.AutomationRuleRunPlaybookAction;
import com.azure.resourcemanager.securityinsights.models.AutomationRuleTriggeringLogic;
import com.azure.resourcemanager.securityinsights.models.PlaybookActionProperties;
import com.azure.resourcemanager.securityinsights.models.ScheduledAlertRule;
import com.azure.resourcemanager.securityinsights.models.SentinelOnboardingState;
import com.azure.resourcemanager.securityinsights.models.TriggerOperator;
import com.azure.resourcemanager.securityinsights.models.TriggersOn;
import com.azure.resourcemanager.securityinsights.models.TriggersWhen;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureMonitoringService {

  private static final Logger logger = LoggerFactory.getLogger(AzureMonitoringService.class);

  private static final int LOG_DATA_RETENTION_DAYS = 90;
  private static final Duration METRIC_GRANULARITY = Duration.ofMinutes(5);
  private static final String UNAUTHORIZED_ACCESS_ALERT_NAME = "UnauthorizedAccess";
  private static final String SENTINEL_ONBOARD_STATE_NAME = "default";
  private static final String SLACK_ALERT_RULE_NAME = "runSendSlackNotificationPlaybook";
  // Leaving this as static as opposed to config since these do not change from one environment to
  // the next
  private static final List<String> TABLES_TO_EXPORT =
      List.of(
          "Alert",
          "AppCenterError",
          "AzureMetrics",
          "ComputerGroup",
          "InsightsMetrics",
          "Operation",
          "StorageBlobLogs",
          "Usage");

  private final AzureResourceConfiguration resourceConfiguration;

  @Autowired
  public AzureMonitoringService(AzureResourceConfiguration resourceConfiguration) {
    this.resourceConfiguration = resourceConfiguration;
  }

  /**
   * Retrieve an existing Log Analytics workspace
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being logged
   * @return A {@link Workspace} object representing the Log Analytics workspace, or null if it does
   *     not exist
   */
  public Workspace getLogAnalyticsWorkspace(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());
    try {
      Workspace byResourceGroup =
          client
              .workspaces()
              .getByResourceGroup(
                  storageAccount.getApplicationResource().getAzureResourceGroupName(),
                  storageAccount.getName());
      logger.info("Found Log Analytics Workspace");
      return byResourceGroup;
    } catch (ManagementException e) {
      logger.warn("No Log Analytics Workspace found", e);
      if (Objects.equals(e.getValue().getCode(), RESOURCE_NOT_FOUND_CODE)) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Create a new Log Analytics workspace
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being logged
   * @return A string representing the id of the Log Analytics workspace that was created
   */
  public String createLogAnalyticsWorkspace(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info(
        "Creating new Log Analytics Workspace for Storage Account {}",
        storageAccount.getStorageAccountId());
    return client
        .workspaces()
        .define(storageAccount.getName())
        .withRegion(Region.fromName(storageAccount.getRegion().getValue()))
        .withExistingResourceGroup(
            storageAccount.getApplicationResource().getAzureResourceGroupName())
        .withSku(new WorkspaceSku().withName(WorkspaceSkuNameEnum.PER_GB2018))
        .withRetentionInDays(LOG_DATA_RETENTION_DAYS)
        .create()
        .id();
  }

  /**
   * Delete an existing Log Analytics workspace
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param id The Azure id of the Log Analytics workspace to delete
   */
  public void deleteLogAnalyticsWorkspace(BillingProfileModel profileModel, String id) {
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info("Deleting Log Analytics Workspace {}", id);

    client.workspaces().deleteById(id);
  }

  /**
   * Retrieve an existing Log Analytics diagnostic setting
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being logged
   * @return A {@link DiagnosticSetting} object representing the storage account diagnostic setting
   *     or null if it does not exist
   */
  public DiagnosticSetting getDiagnosticSetting(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    AzureResourceManager client = resourceConfiguration.getClient(profileModel.getSubscriptionId());

    try {
      DiagnosticSetting diagnosticSetting =
          client
              .diagnosticSettings()
              .get(getStorageAccountLoggingResourceId(storageAccount), storageAccount.getName());
      logger.debug("Found Log Analytics Workspace Diagnostic settings");
      return diagnosticSetting;
    } catch (ManagementException e) {
      logger.debug("No Log Analytics Workspace Diagnostic settings found", e);
      if (Objects.equals(e.getValue().getCode(), RESOURCE_NOT_FOUND_CODE)) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Create a new Log Analytics diagnostic setting
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being logged
   * @param workspaceId ID of the Log Analytics workspace that the diagnostic setting is associated
   *     with
   * @return A string representing the id of the storage account diagnostic setting that was created
   */
  public String createDiagnosticSetting(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccount,
      String workspaceId) {
    AzureResourceManager client = resourceConfiguration.getClient(profileModel.getSubscriptionId());

    logger.info(
        "Creating new Log Analytics Workspace Diagnostic settings for Storage Account {}",
        storageAccount.getStorageAccountId());

    // Obtain all categories that can be tracked
    List<DiagnosticSettingsCategory> logCategories =
        client
            .diagnosticSettings()
            .listCategoriesByResource(getStorageAccountLoggingResourceId(storageAccount))
            .stream()
            .toList();

    return client
        .diagnosticSettings()
        .define(storageAccount.getName())
        .withResource(getStorageAccountLoggingResourceId(storageAccount))
        .withLogAnalytics(workspaceId)
        .withLogsAndMetrics(logCategories, METRIC_GRANULARITY, LOG_DATA_RETENTION_DAYS)
        .create()
        .id();
  }

  /**
   * Delete an existing Log Analytics diagnostic setting
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param id The Azure id of the Log Analytics diagnostic setting to delete
   */
  public void deleteDiagnosticSetting(BillingProfileModel profileModel, String id) {
    AzureResourceManager client = resourceConfiguration.getClient(profileModel.getSubscriptionId());

    logger.info("Deleting Log Analytics Workspace Diagnostic settings {}", id);

    client.diagnosticSettings().deleteById(id);
  }

  /**
   * Retrieve an existing data export rule for the given Log Analytics workspace
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being logged
   * @return A {@link DataExport} object representing the data export rule or null if it does not
   *     exist
   */
  public DataExport getDataExportRule(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {

    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    try {
      DataExport dataExport =
          client
              .dataExports()
              .get(
                  storageAccount.getApplicationResource().getAzureResourceGroupName(),
                  storageAccount.getName(),
                  storageAccount.getName());
      logger.debug("Found Log Analytics Workspace data export rule");
      return dataExport;
    } catch (ManagementException e) {
      logger.debug("No Log Analytics Workspace data export rule found", e);
      // This Azure SDK returns 404 differently than the others
      if (e.getResponse().getStatusCode() == 404) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Create a data export rule for the given Log Analytics workspace for long term log storage
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being logged
   * @return A string representing the id of the object representing the data export rule that was
   *     created
   * @throws IllegalArgumentException if no log collection config is found for the storage account's
   *     region
   */
  public String createDataExportRule(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {

    String resourceId =
        resourceConfiguration
            .getMonitoring()
            .getLogCollectionConfigsAsMap()
            .get(storageAccount.getRegion());
    if (resourceId == null) {
      throw new IllegalArgumentException(
          String.format(
              "No log collection config found for region %s", storageAccount.getRegion()));
    }
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info(
        "Creating new export rule for Log Analytics Workspace linked to Storage Account {}",
        storageAccount.getStorageAccountId());

    return client
        .dataExports()
        .define(storageAccount.getName())
        .withExistingWorkspace(
            storageAccount.getApplicationResource().getAzureResourceGroupName(),
            storageAccount.getName())
        .withTableNames(TABLES_TO_EXPORT)
        .withResourceId(resourceId)
        .create()
        .id();
  }

  /**
   * Delete a data export rule for the given Log Analytics workspace
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param id The Azure id of the data export rule to delete
   */
  public void deleteDataExportRule(BillingProfileModel profileModel, String id) {
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info("Deleting Log Analytics Workspace data export rule {}", id);

    client.dataExports().deleteById(id);
  }

  /**
   * Retrieve a Sentinel instance
   *
   * @param profileModel The billing profile for the dataset or snapshot being monitored
   * @param storageAccount The Storage Account being monitored
   * @return A {@link SentinelOnboardingState} object representing the Sentinel instance or null if
   *     it does not exist
   */
  public SentinelOnboardingState getSentinel(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    try {
      SentinelOnboardingState sentinelOnboardingState =
          client
              .sentinelOnboardingStates()
              .get(
                  storageAccount.getApplicationResource().getAzureResourceGroupName(),
                  storageAccount.getName(),
                  SENTINEL_ONBOARD_STATE_NAME);
      logger.debug("Found Sentinel instance");
      return sentinelOnboardingState;
    } catch (ManagementException e) {
      logger.debug("No Sentinel instance found", e);
      if (Objects.equals(e.getValue().getCode(), RESOURCE_NOT_FOUND_CODE)
          || Objects.equals(e.getValue().getCode(), NOT_FOUND_CODE)) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Create a new Sentinel instance
   *
   * @param profileModel The billing profile for the dataset or snapshot being monitored
   * @param storageAccount The Storage Account being monitored
   * @return A string representing the Sentinel instance that was created
   */
  public String createSentinel(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info(
        "Creating new Sentinel deployment for Log Analytics Workspace that monitors Storage Account {}",
        storageAccount.getStorageAccountId());
    // Need to register `Microsoft.SecurityInsights`, `Microsoft.OperationsManagement` resource
    // providers on the customer subscription before we can create a Sentinel instance
    // TODO: this shows up as throwing a 401 or a 400.  Throw a more specific exception in that case
    // to help the users debug
    // Just create (if it already exists, the request has no effect)
    return client
        .sentinelOnboardingStates()
        .define(SENTINEL_ONBOARD_STATE_NAME)
        .withExistingWorkspace(
            storageAccount.getApplicationResource().getAzureResourceGroupName(),
            storageAccount.getName())
        .create()
        .id();
  }

  /**
   * Delete a Sentinel instance
   *
   * @param profileModel The billing profile for the dataset or snapshot being monitored
   * @param id The azure id of the Sentinel instance to delete
   */
  public void deleteSentinel(BillingProfileModel profileModel, String id) {

    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info("Deleting Sentinel instance {}", id);

    client.sentinelOnboardingStates().deleteById(id);
  }

  /**
   * Get the Sentinel alert rule for the Sentinel instance monitoring the given Storage Account that
   * will alert if too many unauthorized access attempts are made on said Storage Account
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being monitored
   * @return A {@link ScheduledAlertRule} object representing the Sentinel rule that was created
   */
  public AlertRule getSentinelRuleUnauthorizedAccess(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());
    try {
      AlertRule alertRule =
          client
              .alertRules()
              .get(
                  storageAccount.getApplicationResource().getAzureResourceGroupName(),
                  storageAccount.getName(),
                  UNAUTHORIZED_ACCESS_ALERT_NAME);
      logger.debug("Found Sentinel UnauthorizedAccess alert rule");
      return alertRule;
    } catch (ManagementException e) {
      logger.debug("No Sentinel UnauthorizedAccess alert rule found", e);
      if (Objects.equals(e.getValue().getCode(), RESOURCE_NOT_FOUND_CODE)
          || Objects.equals(e.getValue().getCode(), NOT_FOUND_CODE)
          || (Objects.equals(e.getValue().getCode(), BAD_REQUEST_CODE)
              && e.getMessage().contains("is not onboarded to Microsoft Sentinel"))) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Create the Sentinel alert rule for the Sentinel instance monitoring the given Storage Account
   * that will alert if too many unauthorized access attempts are made on said Storage Account
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being monitored
   * @return A string representing the id of the Sentinel rule that was created
   */
  public String createSentinelRuleUnauthorizedAccess(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    // Note: this is copied from the rule defined in the Terra landing zone service
    logger.info(
        "Creating new Sentinel rule for Sentinel instance monitoring Storage Account {} - UnauthorizedAccess",
        storageAccount.getStorageAccountId());
    return client
        .alertRules()
        .createOrUpdate(
            storageAccount.getApplicationResource().getAzureResourceGroupName(),
            storageAccount.getName(),
            "UnauthorizedAccess",
            new ScheduledAlertRule()
                .withDisplayName("File access attempts by unauthorized user accounts")
                .withQuery(
                    """
            StorageBlobLogs\s
            | where StatusCode in (401,403)
            | extend CallerIpAddress = tostring(split(CallerIpAddress, ":")[0]),
                     Identity = coalesce(parse_urlquery(Uri)["Query Parameters"]["rscd"], AuthenticationHash, AuthenticationType)
            | summarize
                Attempts = count(), TimeStart = min(TimeGenerated), TimeEnd = max(TimeGenerated)
                by AccountName, CallerIpAddress, Identity, bin(TimeGenerated, 10m)
            | where Attempts > 10
            | project TimeStart, TimeEnd, Attempts, CallerIpAddress, AccountName, Identity
                                        """)
                .withSuppressionEnabled(false)
                .withSuppressionDuration(Duration.parse("PT1H"))
                .withQueryPeriod(Duration.ofDays(1))
                .withQueryFrequency(Duration.ofDays(1))
                .withEnabled(true)
                .withSeverity(AlertSeverity.INFORMATIONAL)
                .withTriggerOperator(TriggerOperator.GREATER_THAN)
                .withTriggerThreshold(0))
        .id();
  }

  /**
   * Delete the Sentinel alert rule for the Sentinel instance monitoring the given Storage Account.
   * Note: this is for the rule that track unauthorized access to the storage account's files
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being monitored
   */
  public void deleteSentinelRuleUnauthorizedAccess(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info(
        "Deleting Sentinel UnauthorizedAccess alert rule for Storage Account {}",
        storageAccount.getStorageAccountId());
    client
        .alertRules()
        .delete(
            storageAccount.getApplicationResource().getAzureResourceGroupName(),
            storageAccount.getName(),
            UNAUTHORIZED_ACCESS_ALERT_NAME);
  }

  /**
   * Retrieve the rule for sending Slack notifications
   *
   * @param profileModel The billing profile for the dataset or snapshot being monitored
   * @param storageAccount The Storage Account being monitored
   * @return A {@link AutomationRule} object representing the Sentinel notification rule or null if
   *     it doesn't exist
   */
  public AutomationRule getNotificationRule(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());
    try {
      AutomationRule automationRule =
          client
              .automationRules()
              .get(
                  storageAccount.getApplicationResource().getAzureResourceGroupName(),
                  storageAccount.getName(),
                  SLACK_ALERT_RULE_NAME);
      logger.debug("Found Sentinel alert rule");
      return automationRule;
    } catch (ManagementException e) {
      logger.debug("No Sentinel alert rule", e);
      if (Objects.equals(e.getValue().getCode(), RESOURCE_NOT_FOUND_CODE)
          || Objects.equals(e.getValue().getCode(), NOT_FOUND_CODE)) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Create the rule for sending Slack notifications. The Logic App that sends the notifications has
   * an ID configured in the application configuration: azure.monitoring.notificationApplicationId.
   *
   * <p>The app is defined as part of the Azure Terraform <a
   * href="https://github.com/broadinstitute/terraform-ap-deployments/blob/master/azure/terra-tenant/sentinel.tf#L26">here</a>
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccount The Storage Account being monitored
   * @return A string representing the Sentinel rule that was created
   */
  public String createNotificationRule(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccount) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());
    logger.info(
        "Creating new Sentinel alert rule for Sentinel instance monitoring Storage Account {}",
        storageAccount.getStorageAccountId());
    AutomationRuleTriggeringLogic trigger =
        new AutomationRuleTriggeringLogic()
            .withTriggersWhen(TriggersWhen.CREATED)
            .withTriggersOn(TriggersOn.INCIDENTS)
            .withIsEnabled(true);
    AutomationRuleRunPlaybookAction runPlaybookAction =
        new AutomationRuleRunPlaybookAction()
            // There should ever only be one of these
            .withOrder(1)
            .withActionConfiguration(
                new PlaybookActionProperties()
                    .withLogicAppResourceId(
                        resourceConfiguration.getMonitoring().getNotificationApplicationId())
                    .withTenantId(resourceConfiguration.getCredentials().getHomeTenantId()));
    return client
        .automationRules()
        .define(SLACK_ALERT_RULE_NAME)
        .withExistingWorkspace(
            storageAccount.getApplicationResource().getAzureResourceGroupName(),
            storageAccount.getName())
        .withDisplayName("Run Slack Notification Playbook")
        .withOrder(1)
        .withTriggeringLogic(trigger)
        .withActions(List.of(runPlaybookAction))
        .create()
        .id();
  }

  /**
   * Delete the rule for sending Slack notifications
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param id The Azure id of the notification rule to delete
   */
  public void deleteNotificationRule(BillingProfileModel profileModel, String id) {
    SecurityInsightsManager client =
        resourceConfiguration.getSecurityInsightsManagerClient(
            resourceConfiguration.getCredentials().getHomeTenantId(),
            profileModel.getSubscriptionId());

    logger.info("Deleting Sentinel alert rule {}", id);

    client.automationRules().deleteById(id);
  }

  private String getStorageAccountLoggingResourceId(AzureStorageAccountResource storageAccount) {
    return storageAccount.getStorageAccountId() + "/blobServices/default";
  }
}
