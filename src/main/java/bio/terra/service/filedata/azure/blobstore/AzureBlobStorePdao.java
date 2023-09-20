package bio.terra.service.filedata.azure.blobstore;

import static bio.terra.service.filedata.DrsService.getLastNameFromPath;
import static bio.terra.service.filedata.google.gcs.GcsConstants.USER_PROJECT_QUERY_PARAM;
import static bio.terra.service.filedata.google.gcs.GcsPdao.getProjectIdFromGsPath;

import bio.terra.common.UriUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileMetadataUtils.Md5ValidationResult;
import bio.terra.service.filedata.azure.util.AzureBlobStoreBufferedReader;
import bio.terra.service.filedata.azure.util.AzureBlobStoreBufferedWriter;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobContainerCopier;
import bio.terra.service.filedata.azure.util.BlobContainerCopyInfo;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import com.azure.core.credential.TokenCredential;
import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureBlobStorePdao implements CloudFileReader {

  private static final Logger logger = LoggerFactory.getLogger(AzureBlobStorePdao.class);

  public static final Duration DEFAULT_SAS_TOKEN_EXPIRATION = Duration.ofHours(24);

  private static final int LOG_RETENTION_DAYS = 90;

  private static final Set<String> VALID_TLDS =
      Set.of("blob.core.windows.net", "dfs.core.windows.net");

  private final ProfileDao profileDao;
  private final AzureContainerPdao azureContainerPdao;
  private final AzureResourceConfiguration resourceConfiguration;
  private final AzureResourceDao azureResourceDao;
  private final AzureAuthService azureAuthService;
  private final FileIdService fileIdService;
  private final GcsProjectFactory gcsProjectFactory;
  private final GcsPdao gcsPdao;

  @Autowired
  public AzureBlobStorePdao(
      ProfileDao profileDao,
      AzureContainerPdao azureContainerPdao,
      AzureResourceConfiguration resourceConfiguration,
      AzureResourceDao azureResourceDao,
      AzureAuthService azureAuthService,
      FileIdService fileIdService,
      GcsProjectFactory gcsProjectFactory,
      GcsPdao gcsPdao) {
    this.profileDao = profileDao;
    this.azureContainerPdao = azureContainerPdao;
    this.resourceConfiguration = resourceConfiguration;
    this.azureResourceDao = azureResourceDao;
    this.azureAuthService = azureAuthService;
    this.fileIdService = fileIdService;
    this.gcsProjectFactory = gcsProjectFactory;
    this.gcsPdao = gcsPdao;
  }

  private RequestRetryOptions getRetryOptions() {
    return new RequestRetryOptions(
        RetryPolicyType.EXPONENTIAL,
        this.resourceConfiguration.maxRetries(),
        this.resourceConfiguration.retryTimeoutSeconds(),
        null,
        null,
        null);
  }

  public FSFileInfo copyFile(
      Dataset dataset,
      BillingProfileModel profileModel,
      FileLoadModel fileLoadModel,
      String fileId,
      AzureStorageAccountResource storageAccountResource,
      AuthenticatedUserRequest userRequest) {

    SourceFileInfo sourceFileInfo =
        getSourceFileInfo(profileModel.getTenantId(), fileLoadModel.getSourcePath());
    BlobContainerClientFactory targetClientFactory =
        getTargetDataClientFactory(
            profileModel,
            storageAccountResource,
            new BlobSasTokenOptions(
                DEFAULT_SAS_TOKEN_EXPIRATION,
                new BlobSasPermission()
                    .setReadPermission(true)
                    .setListPermission(true)
                    .setWritePermission(true),
                userRequest.getEmail()));

    BlobCrl blobCrl = getBlobCrl(targetClientFactory);

    // Read the leaf node of the source file to use as a way to name the file we store
    String fileName = getLastNameFromPath(UriUtils.toUri(fileLoadModel.getSourcePath()).getPath());

    Md5ValidationResult finalMd5 =
        FileMetadataUtils.validateFileMd5ForIngest(
            fileLoadModel.getMd5(), sourceFileInfo.md5(), fileLoadModel.getSourcePath());

    String effectiveFileId;
    if (fileId == null) {
      effectiveFileId =
          fileIdService
              .calculateFileId(
                  dataset,
                  new FSItem()
                      .path(fileLoadModel.getTargetPath())
                      .checksumMd5(finalMd5.effectiveMd5())
                      .size(sourceFileInfo.size()))
              .toString();
    } else {
      effectiveFileId = fileId;
    }

    String blobName = getBlobName(effectiveFileId, fileName);
    BlobContainerCopier blobContainerCopier;
    if (sourceFileInfo.sourceClientFactory() != null) {
      blobContainerCopier =
          blobCrl.createBlobContainerCopier(
              sourceFileInfo.sourceClientFactory(), sourceFileInfo.sourceUrl(), blobName);
    } else {
      // In the case sourceClientFactory is null, this indicates that we are loading from a gs URI
      blobContainerCopier =
          blobCrl.createBlobContainerCopier(UriUtils.toUri(sourceFileInfo.sourceUrl()), blobName);
    }
    Instant startTime = Instant.now();
    logger.info("Starting copy operation for {}", blobName);
    PollResponse<BlobContainerCopyInfo> copyResponse =
        blobContainerCopier.beginCopyOperation().waitForCompletion();
    logger.info(
        "Finished copy operation for {} with status {} in {} seconds",
        blobName,
        copyResponse.getStatus(),
        Duration.between(startTime, Instant.now()).getSeconds());

    if (!copyResponse.getStatus().equals(LongRunningOperationStatus.SUCCESSFULLY_COMPLETED)) {
      throw new PdaoException(
          "Blob %s was not successfully copied with status: %s"
              .formatted(blobName, copyResponse.getStatus()));
    }

    BlobProperties blobProperties = blobCrl.getBlobProperties(blobName);
    Instant createTime = blobProperties.getCreationTime().toInstant();

    String checksumMd5;
    if (finalMd5.isUserProvided()) {
      checksumMd5 = finalMd5.effectiveMd5();
    } else {
      // If the MD5 wasn't set in the cloud and it has been read from the source, explicitly set it
      if (blobProperties.getContentMd5() == null && finalMd5.effectiveMd5() != null) {
        blobCrl.setBlobMd5(blobName, finalMd5.effectiveMd5().getBytes(StandardCharsets.UTF_8));
      }
      checksumMd5 =
          Optional.ofNullable(blobProperties.getContentMd5())
              .map(Hex::encodeHexString)
              .orElse(finalMd5.effectiveMd5());
    }
    return new FSFileInfo()
        .fileId(effectiveFileId)
        .createdDate(createTime.toString())
        .cloudPath(
            String.format(
                "%s/%s",
                targetClientFactory.getBlobContainerClient().getBlobContainerUrl(), blobName))
        .checksumMd5(checksumMd5)
        .userSpecifiedMd5(finalMd5.isUserProvided())
        .size(blobProperties.getBlobSize())
        .bucketResourceId(storageAccountResource.getResourceId().toString());
  }

  private record SourceFileInfo(
      String md5, long size, BlobContainerClientFactory sourceClientFactory, String sourceUrl) {}

  private SourceFileInfo getSourceFileInfo(UUID tenantId, String sourcePath) {
    if (GcsUriUtils.isGsUri(sourcePath)) {
      String projectId = getProjectIdFromGsPath(sourcePath);
      Storage storage = gcsProjectFactory.getStorage(projectId);
      String sanitizedSourcePath =
          UriUtils.omitQueryParameter(sourcePath, USER_PROJECT_QUERY_PARAM);
      Blob sourceBlob = GcsPdao.getBlobFromGsPath(storage, sanitizedSourcePath, projectId);

      return new SourceFileInfo(
          sourceBlob.getMd5ToHexString(), sourceBlob.getSize(), null, sourcePath);
    } else {
      BlobContainerClientFactory sourceClientFactory =
          buildSourceClientFactory(tenantId, sourcePath);
      BlobUrlParts blobUrl = BlobUrlParts.parse(sourcePath);
      BlobProperties sourceBlobProperties =
          sourceClientFactory
              .getBlobContainerClient()
              .getBlobClient(blobUrl.getBlobName())
              .getProperties();
      String md5 =
          Optional.ofNullable(sourceBlobProperties.getContentMd5())
              .map(Hex::encodeHexString)
              .orElse(null);
      return new SourceFileInfo(
          md5, sourceBlobProperties.getBlobSize(), sourceClientFactory, blobUrl.getBlobName());
    }
  }

  /**
   * Gets all the lines from a source Azure blob as a Stream of strings. This stream MUST be
   * explicitly closed by the calling method.
   *
   * @param blobUrl url to a source blob
   * @param tenantId tenantId as String for interface compatibility
   * @return A Stream of String lines.
   */
  @SuppressFBWarnings("OS_OPEN_STREAM")
  @Override
  public Stream<String> getBlobsLinesStream(
      String blobUrl, String tenantId, AuthenticatedUserRequest userRequest) {
    UUID tenantUuid = UUID.fromString(tenantId);
    String signedBlobUrl = getOrSignUrlStringForSourceFactory(blobUrl, tenantUuid, userRequest);
    AzureBlobStoreBufferedReader azureBlobStoreBufferedReader =
        new AzureBlobStoreBufferedReader(signedBlobUrl);
    return azureBlobStoreBufferedReader.lines();
  }

  @Override
  public void validateUserCanRead(
      List<String> sourcePaths,
      String cloudEncapsulationId,
      AuthenticatedUserRequest user,
      Dataset dataset) {
    // This check is not needed for Azure source files because we use signed URLS that by default
    // check those permissions.  But this is needed if ingesting from GCS-hosted files.
    List<String> gsPaths = sourcePaths.stream().filter(GcsUriUtils::isGsUri).toList();

    // Extract the project ids from the userProject query parameter.  There can be 0 or 1 values
    List<String> cloudEncapsulationIds =
        gsPaths.stream()
            .map(p -> UriUtils.getValueFromQueryParameter(p, USER_PROJECT_QUERY_PARAM))
            .filter(Objects::nonNull)
            .distinct()
            .toList();
    if (cloudEncapsulationIds.size() > 1) {
      throw new IllegalArgumentException("Only a single billing project per ingest may be used");
    }

    gcsPdao.validateUserCanRead(
        gsPaths, cloudEncapsulationIds.stream().findFirst().orElse(null), user, dataset);
  }

  public void writeBlobLines(String signedPath, List<String> lines) {
    try (Stream<String> stream = lines.stream()) {
      writeBlobLines(signedPath, stream);
    }
  }

  public void writeBlobLines(String signedPath, Stream<String> lines) {
    var newLine = "\n";
    try (AzureBlobStoreBufferedWriter writer = new AzureBlobStoreBufferedWriter(signedPath)) {
      lines.forEach(
          line -> {
            try {
              writer.write(line);
              writer.write(newLine);
            } catch (IOException ex) {
              throw new AzureResourceException(
                  String.format(
                      "Could not write to Azure file at %s. Line: %s",
                      BlobUrlParts.parse(signedPath).getBlobName(), line),
                  ex);
            }
          });
    } catch (IOException ex) {
      throw new AzureResourceException(
          String.format(
              "Could not write to Azure file at %s", BlobUrlParts.parse(signedPath).getBlobName()),
          ex);
    }
  }

  public BlobContainerClientFactory buildSourceClientFactory(UUID tenantId, String blobUrl) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(blobUrl);
    if (isSignedUrl(blobUrlParts)) {
      return getSourceClientFactory(blobUrl);
    } else {
      // Use application level authentication
      return getSourceClientFactory(
          blobUrlParts.getAccountName(),
          resourceConfiguration.getAppToken(tenantId),
          blobUrlParts.getBlobContainerName());
    }
  }

  public boolean deleteDataFileById(
      String fileId,
      String fileName,
      AzureStorageAccountResource storageAccountResource,
      AuthenticatedUserRequest userRequest) {
    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(storageAccountResource.getProfileId());

    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(
            profileModel,
            storageAccountResource,
            new BlobSasTokenOptions(
                DEFAULT_SAS_TOKEN_EXPIRATION,
                new BlobSasPermission().setReadPermission(true).setDeletePermission(true),
                userRequest.getEmail()));
    String blobName = getBlobName(fileId, fileName);
    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);
    return blobCrl.deleteBlob(blobName);
  }

  public boolean deleteFile(FireStoreFile fireStoreFile, AuthenticatedUserRequest userRequest) {
    AzureStorageAccountResource storageAccountResource =
        azureResourceDao.retrieveStorageAccountById(
            UUID.fromString(fireStoreFile.getBucketResourceId()));

    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(storageAccountResource.getProfileId());
    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(
            profileModel,
            storageAccountResource,
            new BlobSasTokenOptions(
                DEFAULT_SAS_TOKEN_EXPIRATION,
                new BlobSasPermission().setReadPermission(true).setDeletePermission(true),
                userRequest.getEmail()));

    BlobUrlParts blobParts = BlobUrlParts.parse(fireStoreFile.getGspath());
    if (!blobParts.getAccountName().equals(storageAccountResource.getName())) {
      throw new PdaoException(
          String.format(
              "Resource groups between metadata storage and request do not match: %s != %s",
              blobParts.getAccountName(), storageAccountResource.getName()));
    }
    String blobName = blobParts.getBlobName();
    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);
    boolean success = blobCrl.deleteBlob(blobName);
    var parentBlob = Paths.get(blobName).getParent();
    if (parentBlob != null) {
      // Attempt to delete the parent but this should not cause the overall failure of
      // the file
      blobCrl.deleteBlobQuietFailure(parentBlob.toString());
    }
    return success;
  }

  public boolean deleteScratchParquet(
      String blobPath,
      AzureStorageAccountResource storageAccountResource,
      AuthenticatedUserRequest userRequest) {

    String blobUrl =
        String.format(
            "%s/%s",
            storageAccountResource.getStorageAccountUrl(), FolderType.SCRATCH.getPath(blobPath));
    BlobUrlParts blobParts = BlobUrlParts.parse(blobUrl);

    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(storageAccountResource.getProfileId());
    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(
            profileModel,
            storageAccountResource,
            new BlobSasTokenOptions(
                DEFAULT_SAS_TOKEN_EXPIRATION,
                new BlobSasPermission()
                    .setReadPermission(true)
                    .setDeletePermission(true)
                    .setListPermission(true),
                userRequest.getEmail()));

    String blobName = blobParts.getBlobName();
    BlobCrl blobCrl = getBlobCrl(destinationClientFactory);

    return blobCrl.deleteBlobsWithPrefix(blobName);
  }

  /**
   * Given a signed Url, return all children of the path if it is a directory
   *
   * @return A list of signed URLs to the child blobs
   */
  public List<String> listChildren(String signedUrl) {
    var blobName = BlobUrlParts.parse(signedUrl).getBlobName();
    return getSourceClientFactory(signedUrl)
        .getBlobContainerClient()
        // List children of the parquet directory
        .listBlobs(new ListBlobsOptions().setPrefix(blobName + "/"), Duration.ofMinutes(5))
        .stream()
        // Extract the name
        .map(BlobItem::getName)
        // Ignore the empty placeholder file
        .filter(n -> !n.endsWith("/_"))
        // Fully qualify the name by poor-man cloning the original URL and modifying the blob name
        .map(n -> BlobUrlParts.parse(signedUrl).setBlobName(n).toUrl().toString())
        .collect(Collectors.toList());
  }

  public String signFile(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      String url,
      BlobSasTokenOptions blobSasTokenOptions) {
    BlobContainerClientFactory destinationClientFactory =
        getTargetDataClientFactory(profileModel, storageAccountResource, blobSasTokenOptions);

    BlobUrlParts blobParts = BlobUrlParts.parse(url);
    if (!blobParts.getAccountName().equals(storageAccountResource.getName())) {
      throw new PdaoException(
          String.format(
              "Resource groups between metadata storage and request do not match: %s != %s",
              blobParts.getAccountName(), storageAccountResource.getName()));
    }
    String blobName = blobParts.getBlobName();
    return destinationClientFactory
        .getBlobSasUrlFactory()
        .createSasUrlForBlob(blobName, blobSasTokenOptions);
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

  public BlobContainerClientFactory getTargetDataClientFactory(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      BlobSasTokenOptions blobSasTokenOptions) {

    return new BlobContainerClientFactory(
        azureContainerPdao.getDestinationContainerSignedUrl(
            profileModel, storageAccountResource, blobSasTokenOptions),
        getRetryOptions());
  }

  public BlobUrlParts getOrSignUrlForSourceFactory(
      String dataSourceUrl, UUID tenantId, AuthenticatedUserRequest userRequest) {
    String signedURL = getOrSignUrlStringForSourceFactory(dataSourceUrl, tenantId, userRequest);
    return BlobUrlParts.parse(signedURL);
  }

  public String getOrSignUrlStringForSourceFactory(
      String dataSourceUrl, UUID tenantId, AuthenticatedUserRequest userRequest) {
    // parse user provided url to Azure container - can be signed or unsigned
    BlobUrlParts ingestControlFileBlobUrl = BlobUrlParts.parse(dataSourceUrl);
    String blobName = ingestControlFileBlobUrl.getBlobName();

    // during factory build, we check if url is signed
    // if not signed, we generate the sas token
    // when signing, 'tdr' (the Azure app), must be granted permission on the storage account
    // associated with the provided tenant ID
    BlobContainerClientFactory sourceClientFactory =
        buildSourceClientFactory(tenantId, dataSourceUrl);

    // Given the sas token, rebuild a signed url
    BlobSasTokenOptions options =
        new BlobSasTokenOptions(
            DEFAULT_SAS_TOKEN_EXPIRATION,
            new BlobSasPermission().setReadPermission(true),
            userRequest.getEmail());
    return sourceClientFactory.getBlobSasUrlFactory().createSasUrlForBlob(blobName, options);
  }

  public BlobUrlParts getOrSignUrlForTargetFactory(
      String dataSourceUrl,
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccount,
      AuthenticatedUserRequest userRequest) {
    return BlobUrlParts.parse(
        getOrSignUrlStringForTargetFactory(
            dataSourceUrl, profileModel, storageAccount, userRequest));
  }

  public String getOrSignUrlStringForTargetFactory(
      String dataSourceUrl,
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccount,
      AuthenticatedUserRequest userRequest) {
    BlobUrlParts ingestControlFileBlobUrl = BlobUrlParts.parse(dataSourceUrl);
    String blobName = ingestControlFileBlobUrl.getBlobName();

    // Given the sas token, rebuild a signed url
    BlobSasTokenOptions options =
        new BlobSasTokenOptions(
            DEFAULT_SAS_TOKEN_EXPIRATION,
            new BlobSasPermission()
                .setReadPermission(true)
                .setListPermission(true)
                .setWritePermission(true),
            userRequest.getEmail());

    BlobContainerClientFactory targetDataClientFactory =
        getTargetDataClientFactory(profileModel, storageAccount, options);

    return targetDataClientFactory.getBlobSasUrlFactory().createSasUrlForBlob(blobName, options);
  }

  @VisibleForTesting
  BlobCrl getBlobCrl(BlobContainerClientFactory destinationClientFactory) {
    return new BlobCrl(destinationClientFactory);
  }

  @VisibleForTesting
  BlobContainerClientFactory getSourceClientFactory(String url) {
    return new BlobContainerClientFactory(url, getRetryOptions());
  }

  @VisibleForTesting
  BlobContainerClientFactory getSourceClientFactory(
      String accountName, TokenCredential azureCredential, String containerName) {
    return new BlobContainerClientFactory(
        accountName, azureCredential, containerName, getRetryOptions());
  }

  /** Detects if a URL is a signed URL */
  @VisibleForTesting
  static boolean isSignedUrl(BlobUrlParts blobUrlParts) {
    if (VALID_TLDS.stream().noneMatch(h -> blobUrlParts.getHost().toLowerCase().endsWith(h))) {
      return false;
    }
    return !StringUtils.isEmpty(blobUrlParts.getCommonSasQueryParameters().getSignature());
  }

  private String getBlobName(String fileId, String fileName) {
    return FolderType.DATA.getPath("%s/%s".formatted(fileId, fileName));
  }
}
