package bio.terra.datarepo.service.resourcemanagement.azure;

import bio.terra.datarepo.model.BillingProfileModel;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.UUID;
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
   * Return an authenticated DataLake client using key-based authentication
   *
   * @param profileModel The object containing user tenant information
   * @param storageAccountResource The storage account that DataLake client should built from
   * @return an authenticated DataLake client
   */
  public DataLakeServiceClient getDataLakeClient(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    AzureResourceManager client =
        configuration.getClient(UUID.fromString(profileModel.getSubscriptionId()));

    // Obtain a secret key for the associated storage account
    String key =
        client
            .storageAccounts()
            .getByResourceGroup(
                storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
                storageAccountResource.getName())
            .getKeys()
            .get(0)
            .value();

    // Create a data lake client by authenticating using the found key
    return new DataLakeServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(storageAccountResource.getName(), key))
        .endpoint("https://" + storageAccountResource.getName() + ".blob.core.windows.net")
        .buildClient();
  }
}
