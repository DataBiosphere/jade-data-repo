package bio.terra.service.resourcemanagement.azure;

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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AzureMonitoringService {

  private static final Logger logger = LoggerFactory.getLogger(AzureMonitoringService.class);

  private static final int LOG_DATA_RETENTION_DAYS = 90;
  private static final Duration METRIC_GRANULARITY = Duration.ofMinutes(5);
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
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());
    try {
      Workspace byResourceGroup =
          client
              .workspaces()
              .getByResourceGroup(
                  storageAccount.getApplicationResource().getAzureResourceGroupName(),
                  storageAccount.getName());
      logger.debug("Found Log Analytics Workspace");
      return byResourceGroup;
    } catch (ManagementException e) {
      logger.debug("No Log Analytics Workspace found", e);
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
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());

    logger.info(
        "Creating new Log Analytics Workspace for Storage Account {}",
        storageAccount.getStorageAccountId());
    return client
        .workspaces()
        .define(storageAccount.getName())
        .withRegion(Region.fromName(storageAccount.getRegion().getValue()))
        .withExistingResourceGroup(
            storageAccount.getApplicationResource().getAzureResourceGroupName())
        // Bill the user per GB of ingested log/event data
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
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());

    logger.info("Deleting Log Analytics Workspace {}", id);

    client.workspaces().deleteById(id);
  }

  /**
   * Delete an existing Log Analytics workspace
   *
   * @param profileModel The billing profile for the dataset or snapshot being logged
   * @param storageAccountResource The AzureStorageAccountResource associated with the Log Analytics
   *     workspace to delete
   */
  public void deleteLogAnalyticsWorkspace(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());

    logger.info(
        "Deleting Log Analytics Workspace for storage account {}",
        storageAccountResource.getName());

    client
        .workspaces()
        .delete(
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());
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
        // See
        // https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/migrate-to-azure-storage-lifecycle-policy
        // Setting a non-zero retention period is no longer supported
        // Setting a retention period of 0 days means that the logs will be deleted after the
        // workspace default of 90 days
        .withLogsAndMetrics(logCategories, METRIC_GRANULARITY, 0)
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
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());

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
            .monitoring()
            .getLogCollectionConfigsAsMap()
            .get(storageAccount.getRegion());
    if (resourceId == null) {
      throw new IllegalArgumentException(
          String.format(
              "No log collection config found for region %s", storageAccount.getRegion()));
    }
    LogAnalyticsManager client =
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());

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
        resourceConfiguration.getLogAnalyticsManagerClient(profileModel.getSubscriptionId());

    logger.info("Deleting Log Analytics Workspace data export rule {}", id);

    client.dataExports().deleteById(id);
  }

  private String getStorageAccountLoggingResourceId(AzureStorageAccountResource storageAccount) {
    return storageAccount.getStorageAccountId() + "/blobServices/default";
  }
}
