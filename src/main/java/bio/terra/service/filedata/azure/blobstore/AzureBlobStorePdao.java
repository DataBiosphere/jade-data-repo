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
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
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
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class AzureBlobStorePdao {

  private static final Logger logger = LoggerFactory.getLogger(AzureBlobStorePdao.class);

  private static final int LOG_RETENTION_DAYS = 90;

  private static final Set<String> VALID_TLDS =
      Set.of("blob.core.windows.net", "dfs.core.windows.net");

  private final ProfileDao profileDao;
  private final AzureContainerPdao azureContainerPdao;
  private final AzureResourceConfiguration resourceConfiguration;
  private final AzureResourceDao azureResourceDao;
  private final AzureAuthService azureAuthService;

  @Autowired
  public AzureBlobStorePdao(
      ProfileDao profileDao,
      AzureContainerPdao azureContainerPdao,
      AzureResourceConfiguration resourceConfiguration,
      AzureResourceDao azureResourceDao,
      AzureAuthService azureAuthService) {
    this.profileDao = profileDao;
    this.azureContainerPdao = azureContainerPdao;
    this.resourceConfiguration = resourceConfiguration;
    this.azureResourceDao = azureResourceDao;
    this.azureAuthService = azureAuthService;
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
              resourceConfiguration.getAppToken(profileModel.getTenantId()),
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
        .cloudPath(
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
    if (!blobParts.getAccountName().equals(storageAccountResource.getName())) {
      throw new PdaoException(
          String.format(
              "Resource groups between metadata storage and request do not match: %s != %s",
              blobParts.getAccountName(), storageAccountResource.getName()));
    }
    String blobName = blobParts.getBlobName();
    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);
    try {
      blobCrl.deleteBlob(blobName);
      Optional.ofNullable(Paths.get(blobName).getParent())
          .ifPresent(
              p -> {
                try {
                  // Attempt to delete the file's folder
                  blobCrl.deleteBlob(p.toString());
                } catch (BlobStorageException e) {
                  // Attempt to delete the parent but this should not cause the overall failure of
                  // the file
                  logger.warn("Could not delete the blob folder {}", p, e);
                }
              });
      return true;
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
        return false;
      } else {
        throw new PdaoException("Error deleting file", e);
      }
    }
  }

  public String signFile(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      String url) {
    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(profileModel, storageAccountResource, true);

    BlobUrlParts blobParts = BlobUrlParts.parse(url);
    if (!blobParts.getAccountName().equals(storageAccountResource.getName())) {
      throw new PdaoException(
          String.format(
              "Resource groups between metadata storage and request do not match: %s != %s",
              blobParts.getAccountName(), storageAccountResource.getName()));
    }
    String blobName = blobParts.getBlobName();
    return destinationClientFactory.createReadOnlySasUrlForBlob(blobName);
  }

  /**
   * Enable logging for file access. This creates a container named $logs to which access logs for
   * our files are loaded
   *
   * @param profileModel The model to authorize the connection to the storage account
   * @param storageAccountResource The storage account information to log access for
   */
  public void enableFileLogging(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    var client = azureAuthService.getBlobServiceClient(profileModel, storageAccountResource);
    var props = client.getProperties();
    props
        .getLogging()
        .setVersion("2.0")
        .setRead(true)
        .setWrite(true)
        .setDelete(true)
        .getRetentionPolicy()
        .setEnabled(true)
        .setDays(LOG_RETENTION_DAYS);

    client.setProperties(props);
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
