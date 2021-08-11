package bio.terra.service.resourcemanagement.azure;

import bio.terra.model.BillingProfileModel;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Service class for getting Azure authenticated clients */
@Component
public class AzureAuthService {

  private final AzureResourceConfiguration configuration;

  @Autowired
  public AzureAuthService(AzureResourceConfiguration configuration) {
    this.configuration = configuration;
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
    String key = getStorageAccountKey(profileModel, storageAccountResource);

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
    String key = getStorageAccountKey(profileModel, storageAccountResource);

    // Create a blob client by authenticating using the found key
    return new BlobContainerClientBuilder()
        .credential(new StorageSharedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".blob.core.windows.net")
        .containerName(containerName)
        .buildClient();
  }

  /**
   * Return an authenticated {@link TableServiceClient} client using key-based authentication
   *
   * @param profileModel The object containing user tenant information
   * @param storageAccountResource The sa that BlobContainerClient client should be built from
   * @return an authenticated {@link TableServiceClient}
   */
  public TableServiceClient getTableServiceClient(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    // Obtain a secret key for the associated storage account
    String key = getStorageAccountKey(profileModel, storageAccountResource);

    // Create a data lake client by authenticating using the found key
    return new TableServiceClientBuilder()
        .credential(new AzureNamedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".table.core.windows.net")
        .buildClient();
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
    String key = getStorageAccountKey(profileModel, storageAccountResource);

    // Create a data lake client by authenticating using the found key
    return new BlobServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".blob.core.windows.net")
        .buildClient();
  }

  /** Obtain a secret key for the associated storage account */
  private String getStorageAccountKey(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    AzureResourceManager client = configuration.getClient(profileModel.getSubscriptionId());

    return client
        .storageAccounts()
        .getByResourceGroup(
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName())
        .getKeys()
        .get(0)
        .value();
  }
}
