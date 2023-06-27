package bio.terra.service.resourcemanagement.azure;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;
import java.time.OffsetDateTime;
import org.apache.commons.lang3.StringUtils;
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
   * @return A client connection object that can be used to create folders and files
   */
  public BlobContainerClient getOrCreateContainer(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    BlobContainerClient blobContainerClient =
        authService.getBlobContainerClient(
            profileModel, storageAccountResource, storageAccountResource.getTopLevelContainer());

    if (!blobContainerClient.exists()) {
      blobContainerClient.create();
    }
    return blobContainerClient;
  }

  /**
   * Delete a container in an Azure storage account
   *
   * @param profileModel The profile that describes information needed to access the storage account
   * @param storageAccountResource Metadata describing the storage account that contains the
   *     container
   */
  public void deleteContainer(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    BlobContainerClient blobContainerClient =
        authService.getBlobContainerClient(
            profileModel, storageAccountResource, storageAccountResource.getTopLevelContainer());
    blobContainerClient.deleteIfExists();
  }

  public String getDestinationContainerSignedUrl(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      BlobSasTokenOptions blobSasTokenOptions) {

    OffsetDateTime expiryTime = OffsetDateTime.now().plus(blobSasTokenOptions.getDuration());
    SasProtocol sasProtocol = SasProtocol.HTTPS_ONLY;

    // build the token
    BlobServiceSasSignatureValues sasSignatureValues =
        new BlobServiceSasSignatureValues(expiryTime, blobSasTokenOptions.getSasPermissions())
            .setProtocol(sasProtocol)
            // Version is set to a version of the token signing API the supports keys that permit
            // listing files
            .setVersion("2020-04-08");

    if (!StringUtils.isEmpty(blobSasTokenOptions.getContentDisposition())) {
      sasSignatureValues.setContentDisposition(blobSasTokenOptions.getContentDisposition());
    }

    BlobContainerClient containerClient =
        getOrCreateContainer(profileModel, storageAccountResource);
    return String.format(
        "%s?%s",
        containerClient.getBlobContainerUrl(), containerClient.generateSas(sasSignatureValues));
  }
}
