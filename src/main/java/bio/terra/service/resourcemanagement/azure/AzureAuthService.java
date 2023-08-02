package bio.terra.service.resourcemanagement.azure;

import bio.terra.model.BillingProfileModel;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.http.policy.RetryPolicy;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Service class for getting Azure authenticated clients */
@Component
public class AzureAuthService {
  private final Logger logger = LoggerFactory.getLogger(AzureAuthService.class);

  private final AzureResourceConfiguration configuration;
  private final RequestRetryOptions retryOptions;
  private final Map<AzureAuthorizedCacheKey, String> authorizedMap;

  @Autowired
  public AzureAuthService(AzureResourceConfiguration configuration) {
    this.configuration = configuration;
    var maxRetries = configuration.maxRetries();
    var retryTimeoutSeconds = configuration.retryTimeoutSeconds();
    retryOptions =
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL, maxRetries, retryTimeoutSeconds, null, null, null);
    // wrap the cache map with a synchronized map to safely share the cache across threads
    authorizedMap = Collections.synchronizedMap(new PassiveExpiringMap<>(15, TimeUnit.MINUTES));
  }

  /**
   * Return an authenticated {@link DataLakeServiceClient} using key-based authentication
   *
   * @param profileModel The object containing user tenant information
   * @param storageAccountResource The storage account that DataLake client should be built from
   * @return an authenticated DataLake client
   */
  public DataLakeServiceClient getDataLakeClient(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    String key =
        getStorageAccountKey(
            profileModel.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());

    // Create a data lake client by authenticating using the found key
    return new DataLakeServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".dfs.core.windows.net")
        .buildClient();
  }

  /**
   * Return an authenticated {@link BlobContainerClient} client using key-based authentication
   *
   * @param profileModel The object containing user tenant information
   * @param storageAccountResource The sa that BlobContainerClient client should be built from
   * @param containerName The name of the container to create the client for
   * @return an authenticated {@link BlobContainerClient}
   */
  public BlobContainerClient getBlobContainerClient(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      String containerName) {
    // Obtain a secret key for the associated storage account
    String key =
        getStorageAccountKey(
            profileModel.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());

    // Create a blob client by authenticating using the found key
    return new BlobContainerClientBuilder()
        .credential(new StorageSharedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".blob.core.windows.net")
        .containerName(containerName)
        .retryOptions(retryOptions)
        .buildClient();
  }

  /**
   * Return an authenticated {@link TableServiceClient} client using key-based authentication
   *
   * @param subscriptionId The Azure billing profile subscription id
   * @param resourceGroupName The application deployment resource group name for the sa
   * @param storageAccountResourceName The name of the sa that BlobContainerClient client should be
   *     built from
   * @return an authenticated {@link TableServiceClient}
   */
  public TableServiceClient getTableServiceClient(
      UUID subscriptionId, String resourceGroupName, String storageAccountResourceName) {
    // Obtain a secret key for the associated storage account
    String key =
        getStorageAccountKey(subscriptionId, resourceGroupName, storageAccountResourceName);

    // Create a data lake client by authenticating using the found key
    return new TableServiceClientBuilder()
        .credential(new AzureNamedKeyCredential(storageAccountResourceName, key))
        .endpoint("https://" + storageAccountResourceName + ".table.core.windows.net")
        .retryPolicy(new RetryPolicy())
        .buildClient();
  }

  public TableServiceClient getTableServiceClient(AzureStorageAuthInfo storageAuthInfo) {
    return getTableServiceClient(
        storageAuthInfo.getSubscriptionId(),
        storageAuthInfo.getResourceGroupName(),
        storageAuthInfo.getStorageAccountResourceName());
  }

  /**
   * Return an authenticated {@link BlobServiceClient} client using key-based authentication
   *
   * @param profileModel The object containing user tenant information
   * @param storageAccountResource The sa that BlobServiceClient client should be built from
   * @return an authenticated {@link BlobServiceClient}
   */
  public BlobServiceClient getBlobServiceClient(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    // Obtain a secret key for the associated storage account
    String key =
        getStorageAccountKey(
            profileModel.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());

    // Create a data lake client by authenticating using the found key
    return new BlobServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".blob.core.windows.net")
        .buildClient();
  }

  /** Obtain a secret key for the associated storage account */
  private String getStorageAccountKey(
      UUID subscriptionId, String resourceGroupName, String storageAccountResourceName) {

    AzureAuthorizedCacheKey authorizedCacheKey =
        new AzureAuthorizedCacheKey(subscriptionId, resourceGroupName, storageAccountResourceName);
    return authorizedMap.computeIfAbsent(
        authorizedCacheKey,
        val -> {
          AzureResourceManager client = configuration.getClient(subscriptionId);
          return client
              .storageAccounts()
              .getByResourceGroup(resourceGroupName, storageAccountResourceName)
              .getKeys()
              .get(0)
              .value();
        });
  }
}
