package bio.terra.service.filedata.azure.util;

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobContainerCopier {

  private static final int MIN_LIST_OPERATION_TIMEOUT_IN_SECONDS = 10;
  private static final int MIN_POLLING_INTERVAL_IN_SECONDS = 2;
  private static final Logger logger = LoggerFactory.getLogger(BlobContainerCopier.class);
  private final BlobContainerClientFactory destinationClientFactory;

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

    if (this.sourceClientFactory != null) {
      logger.info("Starting copy operation using a container as a source");
      return beginCopyOperationUsingSourceBlobContainerClient();
    }

    if (StringUtils.isNotBlank(this.sourceBlobUrl)) {
      logger.info("Starting copy operation using a signed blob URL as the source");
      return beginCopyOperationUsingBlobSignedUrl();
    }

    throw new IllegalArgumentException(
        "The copy operation can't be started. " + "The source information is missing or invalid");
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

  private BlobContainerCopySyncPoller beginCopyOperationUsingBlobSignedUrl() {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(this.sourceBlobUrl);

    String sourceBlobName = blobUrlParts.getBlobName();
    String destinationBlobName = this.destinationBlobName;
    if (StringUtils.isBlank(this.destinationBlobName)) {
      destinationBlobName = sourceBlobName;
    }

    return new BlobContainerCopySyncPoller(
        Collections.singletonList(
            beginBlobCopyFromSASUrl(sourceBlobName, this.sourceBlobUrl, destinationBlobName)));
  }

  private BlobContainerCopySyncPoller beginCopyOperationUsingSourceBlobContainerClient() {
    if (this.sourceDestinationPairs != null) {
      logger.info("Copy operation using source and destination pairs.");
      return beginCopyOperationUsingDestinationPairs(this.sourceDestinationPairs);
    }

    return new BlobContainerCopySyncPoller(
        beginCopyOperationFromListOfSources(this.blobSourcePrefix));
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
    logger.info("List operation from the source using the prefix: '{}'", this.blobSourcePrefix);

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

    String sourceSASUrl = this.sourceClientFactory.createReadOnlySASUrlForBlob(sourceBlobName);
    return beginBlobCopyFromSASUrl(sourceBlobName, sourceSASUrl, destinationBlobName);
  }

  private SyncPoller<BlobCopyInfo, Void> beginBlobCopyFromSASUrl(
      String sourceName, String sourceSASUrl, String destinationBlobName) {
    if (StringUtils.isBlank(destinationBlobName)) {
      logger.debug(
          "Destination blob name is blank. The source name: {}, will be used.", sourceName);
      destinationBlobName = sourceName;
    }

    BlobClient blobClient =
        this.destinationClientFactory.getBlobContainerClient().getBlobClient(destinationBlobName);

    return blobClient.beginCopy(sourceSASUrl, this.pollingInterval);
  }

  private boolean isSourceBlobEmpty(String sourceName) {
    return this.sourceClientFactory
            .getBlobContainerClient()
            .getBlobClient(sourceName)
            .getProperties()
            .getBlobSize()
        == 0;
  }

  private ListBlobsOptions createListBlobsOptions(String blobPrefix) {
    ListBlobsOptions options = new ListBlobsOptions();
    BlobListDetails details = new BlobListDetails();
    options.setDetails(details);
    options.setPrefix(blobPrefix);
    return options;
  }
}
