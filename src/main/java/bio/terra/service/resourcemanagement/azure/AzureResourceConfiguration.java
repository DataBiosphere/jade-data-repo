package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.BlockingRejectedExecutionHandler;
import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.loganalytics.LogAnalyticsManager;
import com.azure.resourcemanager.securityinsights.SecurityInsightsManager;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/** Configuration for working with Azure resources */
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "azure")
public record AzureResourceConfiguration(
    Credentials credentials,
    Synapse synapse,
    int maxRetries,
    int retryTimeoutSeconds,
    String apiVersion,
    Monitoring monitoring,
    Threading threading) {

  /**
   * Given a user tenant Id, return Azure credentials
   *
   * @param tenantId The ID of a user tenant
   * @return A credential object that can be used to interact with Azure apis
   */
  public TokenCredential getAppToken(final UUID tenantId) {
    return new ClientSecretCredentialBuilder()
        .clientId(credentials.getApplicationId().toString())
        .clientSecret(credentials.getSecret())
        .tenantId(tenantId.toString())
        .build();
  }

  /**
   * Get Azure credentials authenticated against the home tenant. Use this authentication method
   * when accessing resources within a deployed managed application
   *
   * @return A credential object that can be used to interact with Azure apis
   */
  public TokenCredential getAppToken() {
    return getAppToken(credentials.getHomeTenantId());
  }

  /**
   * Get a resource manager client to a user's tenant. This object can then be used to
   * create/destroy resources
   *
   * @param tenantId The ID of the user's tenant
   * @param subscriptionId The ID of the subscription that will be charged for the resources created
   *     with this client
   * @return An authenticated {@link AzureResourceManager} client
   */
  public AzureResourceManager getClient(final UUID tenantId, final UUID subscriptionId) {
    final AzureProfile profile =
        new AzureProfile(tenantId.toString(), subscriptionId.toString(), AzureEnvironment.AZURE);
    return AzureResourceManager.authenticate(getAppToken(tenantId), profile)
        .withSubscription(subscriptionId.toString());
  }

  /**
   * Get a log analytics resource manager client to a user's tenant. This object can then be used to
   * create/destroy resources Note: this is a separate method from the one above because the log
   * analytics client is not GA yet so does not return a generic AzureResourceManager object
   *
   * @param subscriptionId The ID of the subscription that will be charged for the resources created
   *     with this client
   * @return An authenticated {@link LogAnalyticsManager} client
   */
  public LogAnalyticsManager getLogAnalyticsManagerClient(final UUID subscriptionId) {
    final AzureProfile profile =
        new AzureProfile(
            credentials().getHomeTenantId().toString(),
            subscriptionId.toString(),
            AzureEnvironment.AZURE);
    return LogAnalyticsManager.authenticate(getAppToken(), profile);
  }

  /**
   * Get a security insights (e.g. Sentinel) resource manager client to a user's tenant. This object
   * can then be used to create/destroy resources Note: this is a separate method from the one above
   * because the security insights client is not GA yet so does not return a generic
   * AzureResourceManager object
   *
   * @param subscriptionId The ID of the subscription that will be charged for the resources created
   *     with this client
   * @return An authenticated {@link SecurityInsightsManager} client
   */
  public SecurityInsightsManager getSecurityInsightsManagerClient(final UUID subscriptionId) {
    final AzureProfile profile =
        new AzureProfile(
            credentials().getHomeTenantId().toString(),
            subscriptionId.toString(),
            AzureEnvironment.AZURE);
    return SecurityInsightsManager.authenticate(getAppToken(), profile);
  }

  /**
   * Get a resource manager client to a user's subscription but through TDR's tenant.
   *
   * @param subscriptionId The ID of the subscription that will be charged for the resources created
   *     with this client
   * @return An authenticated {@link AzureResourceManager} client
   */
  public AzureResourceManager getClient(final UUID subscriptionId) {
    return getClient(credentials.getHomeTenantId(), subscriptionId);
  }

  /** Information for authenticating the TDR service against user Azure tenants */
  public static class Credentials {
    // The unique UUID of the TDR application
    private UUID applicationId;
    // A valid and current secret (e.g. application password) for the TDR application
    private String secret;
    // The UUID of the tenant to which the application belongs
    private UUID homeTenantId;

    public UUID getApplicationId() {
      return applicationId;
    }

    public void setApplicationId(UUID applicationId) {
      this.applicationId = applicationId;
    }

    public String getSecret() {
      return secret;
    }

    public void setSecret(String secret) {
      this.secret = secret;
    }

    public UUID getHomeTenantId() {
      return homeTenantId;
    }

    public void setHomeTenantId(UUID homeTenantId) {
      this.homeTenantId = homeTenantId;
    }
  }

  public record Synapse(
      String workspaceName,
      String sqlAdminUser,
      String sqlAdminPassword,
      String databaseName,
      String parquetFileFormatName,
      String encryptionKey,
      boolean initialize,
      int connectRetryInterval,
      int connectRetryCount) {}

  public record Threading(int numTableThreads, int maxQueueSize, boolean blockWhenSaturated) {}

  /**
   * The name of a {@link AsyncTaskExecutor} Spring Bean which executes tasks related to Azure
   * Storage Tables.
   */
  public static final String TABLE_THREADPOOL_NAME = "azureTableThreadpool";

  @Bean(TABLE_THREADPOOL_NAME)
  public AsyncTaskExecutor azureTableThreadpool() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(threading().numTableThreads());
    executor.setMaxPoolSize(threading().numTableThreads());
    executor.setKeepAliveSeconds(0);
    executor.setQueueCapacity(threading().maxQueueSize());
    executor.setThreadNamePrefix("az-table-thread-");
    if (threading().blockWhenSaturated()) {
      executor.setRejectedExecutionHandler(new BlockingRejectedExecutionHandler());
    }
    executor.initialize();
    return executor;
  }

  /** Track the monitoring-related configuration */
  public record Monitoring(
      // The resource ID of the Azure Logic app that handles sending Slack notifications
      String notificationApplicationId,
      // The list of regional storage accounts to send long term logs to
      List<LogCollectionConfig> logCollectionConfigs) {

    public Map<AzureRegion, String> getLogCollectionConfigsAsMap() {
      return logCollectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  LogCollectionConfig::region,
                  LogCollectionConfig::targetStorageAccountResourceId));
    }
  }

  /**
   * Configuration for Storage Accounts to send logs to for long term storage. The accounts must be
   * in the same region as the Log Analytics workspace which is why there may be several of these
   * objects in the service configuration
   */
  public record LogCollectionConfig(AzureRegion region, String targetStorageAccountResourceId) {}
}
