package bio.terra.service.filedata.azure.blobstore;

import static bio.terra.service.filedata.DrsService.getLastNameFromPath;

import bio.terra.common.exception.PdaoException;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class AzureBlobStorePdao {
  private static final Set<String> VALID_TLDS =
      Set.of("blob.core.windows.net", "dfs.core.windows.net");

  private final ProfileDao profileDao;
  private final AzureContainerPdao azureContainerPdao;
  private final AzureResourceConfiguration resourceConfiguration;
  private final AzureResourceDao azureResourceDao;

  @Autowired
  public AzureBlobStorePdao(
      ProfileDao profileDao,
      AzureContainerPdao azureContainerPdao,
      AzureResourceConfiguration resourceConfiguration,
      AzureResourceDao azureResourceDao) {
    this.profileDao = profileDao;
    this.azureContainerPdao = azureContainerPdao;
    this.resourceConfiguration = resourceConfiguration;
    this.azureResourceDao = azureResourceDao;
  }

  public FSFileInfo copyFile(
      FileLoadModel fileLoadModel,
      String fileId,
      AzureStorageAccountResource storageAccountResource) {

    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(fileLoadModel.getProfileId());

    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(profileModel, storageAccountResource, false);

    BlobUrlParts blobUrl = BlobUrlParts.parse(fileLoadModel.getSourcePath());
    BlobContainerClientFactory sourceClientFactory;
    if (isSignedUrl(fileLoadModel.getSourcePath())) {
      sourceClientFactory = getSourceClientFactory(fileLoadModel.getSourcePath());
    } else {
      // Use application level authentication
      sourceClientFactory =
          getSourceClientFactory(
              blobUrl.getAccountName(),
              resourceConfiguration.getAppToken(UUID.fromString(profileModel.getTenantId())),
              blobUrl.getBlobContainerName());
    }

    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);

    // Read the leaf node of the source file to use as a way to name the file we store
    String fileName = getLastNameFromPath(blobUrl.getBlobName());

    String blobName = getBlobName(fileId, fileName);
    blobCrl
        .createBlobContainerCopier(sourceClientFactory, blobUrl.getBlobName(), blobName)
        .beginCopyOperation()
        .waitForCompletion();

    BlobProperties blobProperties = blobCrl.getBlobProperties(blobName);
    Instant createTime = blobProperties.getCreationTime().toInstant();
    return new FSFileInfo()
        .fileId(fileId)
        .createdDate(createTime.toString())
        .gspath(
            String.format(
                "%s/%s",
                destinationClientFactory.getBlobContainerClient().getBlobContainerUrl(), blobName))
        .checksumMd5(Base64.getEncoder().encodeToString((blobProperties.getContentMd5())))
        .size(blobProperties.getBlobSize())
        .bucketResourceId(storageAccountResource.getResourceId().toString());
  }

  public boolean deleteDataFileById(
      String fileId, String fileName, AzureStorageAccountResource storageAccountResource) {
    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(storageAccountResource.getProfileId());

    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(profileModel, storageAccountResource, true);
    String blobName = getBlobName(fileId, fileName);
    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);
    try {
      blobCrl.deleteBlob(blobName);
      return true;
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
        return false;
      } else {
        throw new PdaoException("Error deleting file", e);
      }
    }
  }

  public boolean deleteFile(FireStoreFile fireStoreFile) {
    AzureStorageAccountResource storageAccountResource =
        azureResourceDao.retrieveStorageAccountById(
            UUID.fromString(fireStoreFile.getBucketResourceId()));

    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(storageAccountResource.getProfileId());
    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(profileModel, storageAccountResource, true);

    BlobUrlParts blobParts = BlobUrlParts.parse(fireStoreFile.getGspath());
    if (!StringUtils.equals(blobParts.getAccountName(), storageAccountResource.getName())) {
      throw new PdaoException(
          String.format(
              "Resource groups between metadata storage and request do not match: %s != %s",
              blobParts.getAccountName(), storageAccountResource.getName()));
    }
    String blobName = blobParts.getBlobName();
    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);
    try {
      blobCrl.deleteBlob(blobName);
      return true;
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
        return false;
      } else {
        throw new PdaoException("Error deleting file", e);
      }
    }
  }

  @VisibleForTesting
  BlobContainerClientFactory getTargetDataClientFactory(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      boolean enableDelete) {
    return new BlobContainerClientFactory(
        azureContainerPdao.getDestinationContainerSignedUrl(
            profileModel,
            storageAccountResource,
            ContainerType.DATA,
            true,
            true,
            true,
            enableDelete));
  }

  @VisibleForTesting
  BlobCrl getBlobCrl(BlobContainerClientFactory destinationClientFactory) {
    return new BlobCrl(destinationClientFactory);
  }

  @VisibleForTesting
  BlobContainerClientFactory getSourceClientFactory(String sourceUrl) {
    return new BlobContainerClientFactory(sourceUrl);
  }

  @VisibleForTesting
  BlobContainerClientFactory getSourceClientFactory(
      String accountName, TokenCredential azureCredential, String containerName) {
    return new BlobContainerClientFactory(accountName, azureCredential, containerName);
  }

  /** Detects if a URL is a signed URL */
  @VisibleForTesting
  static boolean isSignedUrl(String url) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(url);

    if (VALID_TLDS.stream().noneMatch(h -> blobUrlParts.getHost().toLowerCase().endsWith(h))) {
      return false;
    }
    return !StringUtils.isEmpty(blobUrlParts.getCommonSasQueryParameters().getSignature());
  }

  private String getBlobName(String fileId, String fileName) {
    return String.format("%s/%s", fileId, fileName);
  }
}
