package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.model.AzureRegion;
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
import org.springframework.context.annotation.Configuration;

/** Configuration for working with Azure resources */
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "azure")
public class AzureResourceConfiguration {
  private Credentials credentials;
  private Synapse synapse;
  private int maxRetries;
  private int retryTimeoutSeconds;
  private String apiVersion;
  private Monitoring monitoring;

  public Credentials getCredentials() {
    return credentials;
  }

  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }

  public Synapse getSynapse() {
    return synapse;
  }

  public void setSynapse(Synapse synapse) {
    this.synapse = synapse;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public int getRetryTimeoutSeconds() {
    return retryTimeoutSeconds;
  }

  public void setRetryTimeoutSeconds(int retryTimeoutSeconds) {
    this.retryTimeoutSeconds = retryTimeoutSeconds;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public void setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
  }

  public Monitoring getMonitoring() {
    return monitoring;
  }

  public void setMonitoring(Monitoring monitoring) {
    this.monitoring = monitoring;
  }

  /**
   * Given a user tenant Id, return Azure credentials
   *
   * @param tenantId The ID of a user tenant
   * @return A credential object that can be used to interact with Azure apis
   */
  public TokenCredential getAppToken(final UUID tenantId) {
    return new ClientSecretCredentialBuilder()
        .clientId(credentials.applicationId.toString())
        .clientSecret(credentials.secret)
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
   * @param tenantId The ID of the user's tenant
   * @param subscriptionId The ID of the subscription that will be charged for the resources created
   *     with this client
   * @return An authenticated {@link LogAnalyticsManager} client
   */
  public LogAnalyticsManager getLogAnalyticsManagerClient(
      final UUID tenantId, final UUID subscriptionId) {
    final AzureProfile profile =
        new AzureProfile(tenantId.toString(), subscriptionId.toString(), AzureEnvironment.AZURE);
    return LogAnalyticsManager.authenticate(getAppToken(), profile);
  }

  /**
   * Get a security insights (e.g. Sentinel) resource manager client to a user's tenant. This object
   * can then be used to create/destroy resources Note: this is a separate method from the one above
   * because the security insights client is not GA yet so does not return a generic
   * AzureResourceManager object
   *
   * @param tenantId The ID of the user's tenant
   * @param subscriptionId The ID of the subscription that will be charged for the resources created
   *     with this client
   * @return An authenticated {@link SecurityInsightsManager} client
   */
  public SecurityInsightsManager getSecurityInsightsManagerClient(
      final UUID tenantId, final UUID subscriptionId) {
    final AzureProfile profile =
        new AzureProfile(tenantId.toString(), subscriptionId.toString(), AzureEnvironment.AZURE);
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

  public static class Synapse {

    private String workspaceName;
    private String sqlAdminUser;
    private String sqlAdminPassword;
    private String databaseName;
    private String parquetFileFormatName;
    private String encryptionKey;
    private boolean initialize;

    public String getWorkspaceName() {
      return workspaceName;
    }

    public void setWorkspaceName(String workspaceName) {
      this.workspaceName = workspaceName;
    }

    public String getSqlAdminUser() {
      return sqlAdminUser;
    }

    public void setSqlAdminUser(String sqlAdminUser) {
      this.sqlAdminUser = sqlAdminUser;
    }

    public String getSqlAdminPassword() {
      return sqlAdminPassword;
    }

    public void setSqlAdminPassword(String sqlAdminPassword) {
      this.sqlAdminPassword = sqlAdminPassword;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public void setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
    }

    public String getParquetFileFormatName() {
      return parquetFileFormatName;
    }

    public void setParquetFileFormatName(String parquetFileFormatName) {
      this.parquetFileFormatName = parquetFileFormatName;
    }

    public String getEncryptionKey() {
      return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
      this.encryptionKey = encryptionKey;
    }

    public boolean isInitialize() {
      return initialize;
    }

    public void setInitialize(boolean initialize) {
      this.initialize = initialize;
    }
  }

  /** Track the monitoring-related configuration */
  public static class Monitoring {
    // The resource ID of the Azure Logic app that handles sending Slack notifications
    private String notificationApplicationId;
    // The list of regional storage accounts to send long term logs to
    private List<LogCollectionConfig> logCollectionConfigs;

    public String getNotificationApplicationId() {
      return notificationApplicationId;
    }

    public void setNotificationApplicationId(String notificationApplicationId) {
      this.notificationApplicationId = notificationApplicationId;
    }

    public List<LogCollectionConfig> getLogCollectionConfigs() {
      return logCollectionConfigs;
    }

    public void setLogCollectionConfigs(List<LogCollectionConfig> logCollectionConfigs) {
      this.logCollectionConfigs = logCollectionConfigs;
    }

    public Map<AzureRegion, String> getLogCollectionConfigsAsMap() {
      return logCollectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  LogCollectionConfig::getRegion,
                  LogCollectionConfig::getTargetStorageAccountResourceId));
    }
  }

  /**
   * Configuration for Storage Accounts to send logs to for long term storage. The accounts must be
   * in the same region as the Log Analytics workspace which is why there may be several of these
   * objects in the service configuration
   */
  public static class LogCollectionConfig {

    private AzureRegion region;
    private String targetStorageAccountResourceId;

    public AzureRegion getRegion() {
      return region;
    }

    public void setRegion(String region) {
      AzureRegion azureRegion = AzureRegion.fromValue(region);
      if (azureRegion == null) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid region '%s' specified in azure.monitoring.logCollectionConfigs", region));
      }
      this.region = azureRegion;
    }

    public String getTargetStorageAccountResourceId() {
      return targetStorageAccountResourceId;
    }

    public void setTargetStorageAccountResourceId(String targetStorageAccountResourceId) {
      this.targetStorageAccountResourceId = targetStorageAccountResourceId;
    }
  }
}
