package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.google.gcs.GcsPdao.getProjectIdFromGsPath;

import bio.terra.common.UriUtils;
import bio.terra.common.exception.NotFoundException;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsConstants;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.RehydratePriority;
import com.azure.storage.blob.options.BlobBeginCopyOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.SignUrlOption;
import com.google.cloud.storage.StorageOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobContainerCopier {

  private static final int MIN_LIST_OPERATION_TIMEOUT_IN_SECONDS = 10;
  private static final int MIN_POLLING_INTERVAL_IN_SECONDS = 2;
  private static final Logger logger = LoggerFactory.getLogger(BlobContainerCopier.class);
  private static final Duration DEFAULT_SAS_TOKEN_EXPIRATION = Duration.ofHours(24);
  private final BlobContainerClientFactory destinationClientFactory;
  private static final String USER_PROJECT_QUERY_PARAM = "userProject";
  // Signing URL for 48 hours to handle large file transfers
  private static final long SIGNED_URL_DURATION_MINUTES = TimeUnit.HOURS.toMinutes(48);

  private String blobSourcePrefix = "";

  private BlobContainerClientFactory sourceClientFactory;
  private String sourceBlobUrl;
  private String destinationBlobName;
  private Duration listOperationTimeout = Duration.ofSeconds(MIN_LIST_OPERATION_TIMEOUT_IN_SECONDS);
  private Duration pollingInterval = Duration.ofSeconds(MIN_POLLING_INTERVAL_IN_SECONDS);

  private List<BlobCopySourceDestinationPair> sourceDestinationPairs;

  public BlobContainerCopier(BlobContainerClientFactory destinationClientFactory) {
    this.destinationClientFactory =
        Objects.requireNonNull(
            destinationClientFactory, "Destination client factory is required and can't be null");
  }

  /**
   * Begins an asynchronous copy operation between storage accounts. The backend storage service
   * performs the copy operation. The method returns an instance of the {@link
   * BlobContainerCopySyncPoller} which facilitates the management and getting the status of the
   * copy operation.
   *
   * @return an instance of {@link BlobContainerCopySyncPoller} that is associated with the copy
   *     operation.
   */
  public BlobContainerCopySyncPoller beginCopyOperation() {

    if (sourceClientFactory != null) {
      logger.info("Starting copy operation using a container as a source");
      return beginCopyOperationUsingSourceBlobContainerClient();
    }

    if (StringUtils.isNotBlank(sourceBlobUrl)) {
      logger.info("Starting copy operation using a blob URL as the source");
      return beginCopyOperationUsingUrl();
    }

    throw new IllegalArgumentException(
        "The copy operation can't be started. The source information is missing or invalid");
  }

  public void setDestinationBlobName(String destinationBlobName) {
    this.destinationBlobName = destinationBlobName;
  }

  public void setSourceDestinationPairs(
      List<BlobCopySourceDestinationPair> sourceDestinationPairs) {
    this.sourceDestinationPairs = sourceDestinationPairs;
  }

  public void setSourceBlobUrl(String sourceBlobUrl) {
    this.sourceBlobUrl = sourceBlobUrl;
  }

  public void setBlobSourcePrefix(String blobSourcePrefix) {
    this.blobSourcePrefix =
        Objects.requireNonNull(blobSourcePrefix, "Blob source prefix must not be null");
  }

  public void setListOperationTimeout(Duration listOperationTimeout) {
    this.listOperationTimeout = listOperationTimeout;
  }

  public void setPollingInterval(Duration pollingInterval) {
    this.pollingInterval = pollingInterval;
  }

  public void setSourceClientFactory(BlobContainerClientFactory sourceClientFactory) {
    this.sourceClientFactory = sourceClientFactory;
  }

  public BlobContainerClientFactory getDestinationClientFactory() {
    return destinationClientFactory;
  }

  public String getBlobSourcePrefix() {
    return blobSourcePrefix;
  }

  public BlobContainerClientFactory getSourceClientFactory() {
    return sourceClientFactory;
  }

  public String getSourceBlobUrl() {
    return sourceBlobUrl;
  }

  public String getDestinationBlobName() {
    return destinationBlobName;
  }

  public List<BlobCopySourceDestinationPair> getSourceDestinationPairs() {
    return sourceDestinationPairs;
  }

  private BlobContainerCopySyncPoller beginCopyOperationUsingUrl() {
    String sourceBlobName;
    if (GcsUriUtils.isGsUri(sourceBlobUrl)) {
      sourceBlobName = UriUtils.toUri(sourceBlobUrl).getPath();
    } else {
      BlobUrlParts blobUrlParts = BlobUrlParts.parse(sourceBlobUrl);
      sourceBlobName = blobUrlParts.getBlobName();
    }

    String destinationBlobName = this.destinationBlobName;
    if (StringUtils.isBlank(this.destinationBlobName)) {
      destinationBlobName = sourceBlobName;
    }

    return new BlobContainerCopySyncPoller(
        List.of(beginBlobCopyFromUrl(sourceBlobName, sourceBlobUrl, destinationBlobName)));
  }

  private BlobContainerCopySyncPoller beginCopyOperationUsingSourceBlobContainerClient() {
    if (sourceDestinationPairs != null) {
      logger.info("Copy operation using source and destination pairs.");
      return beginCopyOperationUsingDestinationPairs(sourceDestinationPairs);
    }

    return new BlobContainerCopySyncPoller(beginCopyOperationFromListOfSources(blobSourcePrefix));
  }

  private BlobContainerCopySyncPoller beginCopyOperationUsingDestinationPairs(
      List<BlobCopySourceDestinationPair> sourceDestinationPairs) {

    List<BlobCopySourceDestinationPair> pairs =
        Objects.requireNonNull(sourceDestinationPairs, "sourceDestinationInfos must not be null");

    if (pairs.isEmpty()) {
      throw new RuntimeException("The source and destination pair list is empty.");
    }

    return new BlobContainerCopySyncPoller(
        beginCopyOperationFromListOfSourceDestinationPairs(pairs));
  }

  private List<SyncPoller<BlobCopyInfo, Void>> beginCopyOperationFromListOfSources(
      String blobPrefix) {

    ListBlobsOptions options = createListBlobsOptions(blobPrefix);
    logger.info("List operation from the source using the prefix: '{}'", blobSourcePrefix);

    return this.sourceClientFactory
        .getBlobContainerClient()
        .listBlobs(options, listOperationTimeout)
        .stream()
        .map(b -> beginBlobCopy(b.getName(), b.getName()))
        .filter(
            Objects::nonNull) // remove nulls as beginBlobCopy returns null when an empty blob is
        // requested.
        .collect(Collectors.toList());
  }

  private List<SyncPoller<BlobCopyInfo, Void>> beginCopyOperationFromListOfSourceDestinationPairs(
      List<BlobCopySourceDestinationPair> blobCopySourceDestinationPairs) {

    return blobCopySourceDestinationPairs.stream()
        .map(b -> beginBlobCopy(b.getSourceBlobName(), b.getDestinationBlobName()))
        .filter(
            Objects::nonNull) // remove nulls as beginBlobCopy returns null when an empty blob is
        // requested.
        .collect(Collectors.toList());
  }

  private SyncPoller<BlobCopyInfo, Void> beginBlobCopy(
      String sourceBlobName, String destinationBlobName) {

    // Azure blob copy does not support empty files.
    if (isSourceBlobEmpty(sourceBlobName)) {
      logger.warn("Blob {} is empty. Copy operation is not allowed", sourceBlobName);
      return null;
    }

    String sourceSASUrl = createSourceBlobReadOnlySasUrl(sourceBlobName);
    return beginBlobCopyFromUrl(sourceBlobName, sourceSASUrl, destinationBlobName);
  }

  private String createSourceBlobReadOnlySasUrl(String blobName) {

    BlobSasTokenOptions blobSasTokenOptions =
        new BlobSasTokenOptions(
            DEFAULT_SAS_TOKEN_EXPIRATION,
            new BlobSasPermission().setReadPermission(true),
            BlobContainerCopier.class.getName());

    return sourceClientFactory
        .getBlobSasUrlFactory()
        .createSasUrlForBlob(blobName, blobSasTokenOptions);
  }

  private SyncPoller<BlobCopyInfo, Void> beginBlobCopyFromUrl(
      String sourceName, String sourceUrl, String destinationBlobName) {

    String effectiveSourceUrl;
    if (GcsUriUtils.isGsUri(sourceUrl)) {
      effectiveSourceUrl = getGcsFileInfo(sourceUrl).signedUrl();
    } else {
      effectiveSourceUrl = sourceUrl;
    }

    if (StringUtils.isBlank(destinationBlobName)) {
      logger.debug(
          "Destination blob name is blank. The source name: {}, will be used.", sourceName);
      destinationBlobName = sourceName;
    }

    BlobClient blobClient =
        destinationClientFactory.getBlobContainerClient().getBlobClient(destinationBlobName);

    return blobClient.beginCopy(
        new BlobBeginCopyOptions(effectiveSourceUrl)
            .setPollInterval(pollingInterval)
            .setRehydratePriority(RehydratePriority.HIGH));
  }

  record GcsFileInfo(String signedUrl, String md5) {}

  /**
   * Return a file information for a GCS blob to be used to ingest into an Azure blob. Note: this
   * lives in this class since it's use is limited to this particular use case.
   */
  private GcsFileInfo getGcsFileInfo(String gspath) {
    String projectId = getProjectIdFromGsPath(gspath);

    StorageOptions.Builder storageBuilder = StorageOptions.newBuilder();
    BlobGetOption[] getOptions = new BlobGetOption[0];
    SignUrlOption[] signOptions = new SignUrlOption[0];
    if (projectId != null) {
      storageBuilder.setProjectId(projectId);
      getOptions = new BlobGetOption[] {BlobGetOption.userProject(projectId)};
      signOptions =
          new SignUrlOption[] {
            SignUrlOption.withQueryParams(Map.of(USER_PROJECT_QUERY_PARAM, projectId))
          };
    }
    Storage storage = storageBuilder.build().getService();

    String sanitizedUri =
        UriUtils.omitQueryParameter(gspath, GcsConstants.USER_PROJECT_QUERY_PARAM);
    BlobId locator = GcsUriUtils.parseBlobUri(sanitizedUri);
    Blob blob = storage.get(locator, getOptions);
    if (blob == null) {
      throw new NotFoundException("Blob %s could not be read".formatted(gspath));
    }
    return new GcsFileInfo(
        blob.signUrl(SIGNED_URL_DURATION_MINUTES, TimeUnit.MINUTES, signOptions).toString(),
        blob.getMd5());
  }

  private boolean isSourceBlobEmpty(String sourceName) {
    BlobClient client = sourceClientFactory.getBlobContainerClient().getBlobClient(sourceName);
    return client.getProperties().getBlobSize() == 0;
  }

  private ListBlobsOptions createListBlobsOptions(String blobPrefix) {
    ListBlobsOptions options = new ListBlobsOptions();
    BlobListDetails details = new BlobListDetails();
    options.setDetails(details);
    options.setPrefix(blobPrefix);
    return options;
  }
}
