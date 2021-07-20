package bio.terra.service.resourcemanagement.azure;

import bio.terra.model.BillingProfileModel;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;
import java.time.OffsetDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureContainerPdao {

  private final AzureAuthService authService;

  @Autowired
  public AzureContainerPdao(AzureAuthService authService) {
    this.authService = authService;
  }

  /**
   * Get or create a container in an Azure storage account
   *
   * @param profileModel The profile that describes information needed to access the storage account
   * @param storageAccountResource Metadata describing the storage account that contains the
   *     container
   * @param containerType The nature of the container
   * @return A client connection object that can be used to create folders and files
   */
  public BlobContainerClient getOrCreateContainer(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      AzureStorageAccountResource.ContainerType containerType) {
    BlobContainerClient blobContainerClient =
        authService.getBlobContainerClient(
            profileModel,
            storageAccountResource,
            storageAccountResource.determineContainer(containerType));

    if (!blobContainerClient.exists()) {
      blobContainerClient.create();
    }
    return blobContainerClient;
  }

  public String getDestinationContainerSignedUrl(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      AzureStorageAccountResource.ContainerType containerType,
      boolean enableRead,
      boolean enableList,
      boolean enableWrite,
      boolean enableDelete) {

    BlobContainerSasPermission permissions =
        new BlobContainerSasPermission()
            .setReadPermission(enableRead)
            .setWritePermission(enableWrite)
            .setListPermission(enableList)
            .setDeletePermission(enableDelete);

    OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
    SasProtocol sasProtocol = SasProtocol.HTTPS_ONLY;

    // build the token
    BlobServiceSasSignatureValues sasSignatureValues =
        new BlobServiceSasSignatureValues(expiryTime, permissions)
            .setProtocol(sasProtocol)
            // Version is set to a version of the token signing API the supports keys that permit
            // listing files
            .setVersion("2020-04-08");

    BlobContainerClient containerClient =
        getOrCreateContainer(profileModel, storageAccountResource, containerType);
    return String.format(
        "%s?%s",
        containerClient.getBlobContainerUrl(),
        containerClient.generateSas(sasSignatureValues, new Context("FOO", "BAR")));
  }
}
