package bio.terra.service.filedata.azure.util;

import bio.terra.service.common.gcs.GcsUriUtils;
import com.azure.storage.blob.BlobUrlParts;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

/** Builder for the {@link BlobContainerCopier} */
public final class BlobContainerCopierBuilder {

  public static final int DEFAULT_LIST_OPERATION_TIMEOUT_IN_SECONDS = 30;
  public static final int DEFAULT_POLLING_INTERVAL_IN_SECONDS = 5;
  private static final int MIN_POLLING_INTERVAL = 2;
  private static final int MIN_LIST_TIMEOUT = 10;
  private static final String EMPTY_PREFIX = "";

  private BlobContainerClientFactory sourceClientFactory;
  private BlobContainerClientFactory destinationClientFactory;
  private String sourceContainerPrefix = EMPTY_PREFIX;
  private String sourceBlobUrl;
  private Duration pollingInterval = Duration.ofSeconds(DEFAULT_POLLING_INTERVAL_IN_SECONDS);
  private Duration listOperationTimeout =
      Duration.ofSeconds(DEFAULT_LIST_OPERATION_TIMEOUT_IN_SECONDS);
  private List<BlobCopySourceDestinationPair> sourceDestinationPairs;
  private String destinationBlobName;

  public BlobContainerCopierBuilder destinationBlobName(String destinationBlobName) {
    this.destinationBlobName = destinationBlobName;
    return this;
  }

  public BlobContainerCopierBuilder sourceDestinationPairs(
      List<BlobCopySourceDestinationPair> pairs) {
    sourceDestinationPairs = pairs;
    return this;
  }

  public BlobContainerCopierBuilder sourceClientFactory(BlobContainerClientFactory factory) {
    sourceClientFactory = Objects.requireNonNull(factory, "Source client factory can't be null");

    return this;
  }

  public BlobContainerCopierBuilder destinationClientFactory(BlobContainerClientFactory factory) {
    destinationClientFactory =
        Objects.requireNonNull(factory, "Destination client factory can't be null");

    return this;
  }

  public BlobContainerCopierBuilder sourceContainerPrefix(String prefix) {

    sourceContainerPrefix = Objects.requireNonNull(prefix, "The prefix can't be null");

    return this;
  }

  public BlobContainerCopierBuilder sourceBlobUrl(String url) {
    if (GcsUriUtils.isGsUri(url)) {
      GcsUriUtils.validateBlobUri(url);
    } else {
      BlobUrlParts blobUrl = BlobUrlParts.parse(url);

      if (Strings.isEmpty(blobUrl.getBlobContainerName())) {
        throw new IllegalArgumentException(
            appendBlobGuidanceToErrorMessage("Container name is missing."));
      }

      if (Strings.isEmpty(blobUrl.getBlobName())) {
        throw new IllegalArgumentException(
            appendBlobGuidanceToErrorMessage("URL must contain a blob name."));
      }

      String permissions = blobUrl.getCommonSasQueryParameters().getPermissions();

      if (Strings.isEmpty(permissions) || !permissions.contains("r")) {
        throw new IllegalArgumentException(
            appendBlobGuidanceToErrorMessage("Read permission is required."));
      }
    }
    sourceBlobUrl = url;

    return this;
  }

  public String appendBlobGuidanceToErrorMessage(String errorMsg) {
    return errorMsg
        + " The URL format must be:"
        + "https://{account name}.blob.core.windows.net/{container name}/{blob name}"
        + "?{valid SAS with read permissions}";
  }

  public BlobContainerCopierBuilder pollingInterval(Duration pollingInterval) {
    Objects.requireNonNull(pollingInterval, "Invalid polling interval. It can't be null");

    if (pollingInterval.getSeconds() < MIN_POLLING_INTERVAL) {
      throw new IllegalArgumentException(
          "Polling interval can't be less than " + MIN_POLLING_INTERVAL + " seconds");
    }
    this.pollingInterval = pollingInterval;

    return this;
  }

  public BlobContainerCopierBuilder listOperationTimeOut(Duration listOperationTimeout) {
    Objects.requireNonNull(listOperationTimeout, "Invalid timeout value. It can't be null");

    if (listOperationTimeout.getSeconds() < MIN_LIST_TIMEOUT) {
      throw new IllegalArgumentException(
          "List polling timeout can't be less than " + MIN_LIST_TIMEOUT + " seconds");
    }
    this.listOperationTimeout = listOperationTimeout;

    return this;
  }

  public BlobContainerCopier build() {
    BlobContainerCopier copier = new BlobContainerCopier(destinationClientFactory);

    copier.setPollingInterval(pollingInterval);
    copier.setListOperationTimeout(listOperationTimeout);
    copier.setBlobSourcePrefix(sourceContainerPrefix);
    copier.setSourceDestinationPairs(sourceDestinationPairs);

    if (sourceClientFactory != null) {
      copier.setSourceClientFactory(sourceClientFactory);
      return copier;
    }

    if (StringUtils.isNotBlank(sourceBlobUrl)) {
      copier.setSourceBlobUrl(sourceBlobUrl);
      copier.setDestinationBlobName(destinationBlobName);
      return copier;
    }

    throw new IllegalArgumentException(
        "The builder can't create the copier. "
            + "You must provide a valid source blob client manager "
            + "or a blob URL with SAS token with read permission.");
  }
}
