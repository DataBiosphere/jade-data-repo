package bio.terra.service.filedata.azure.util;

import bio.terra.common.exception.PdaoException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

/**
 * High-level API that enables copy, delete, and get blob properties operations on Azure Blob
 * storage.
 */
public class BlobCrl {
  private static final Logger logger = LoggerFactory.getLogger(BlobCrl.class);
  private final BlobContainerClientFactory blobContainerClientFactory;

  public BlobCrl(BlobContainerClientFactory clientFactory) {
    this.blobContainerClientFactory =
        Objects.requireNonNull(clientFactory, "Client factory is null");
  }

  /**
   * Creates a new instance of {@link BlobContainerCopier} that uses the {@link
   * BlobContainerClientFactory} specified in the constructor as the destination storage account.
   *
   * @param clientFactory client factory of the source storage account and container.
   * @param blobPrefix prefix filter to use when listing the sources from the source container. If
   *     blank all blobs in the container will be copied.
   * @return new instance of {@link BlobContainerClientFactory}.
   */
  public BlobContainerCopier createBlobContainerCopier(
      BlobContainerClientFactory clientFactory, String blobPrefix) {
    return new BlobContainerCopierBuilder()
        .destinationClientFactory(blobContainerClientFactory)
        .sourceClientFactory(clientFactory)
        .sourceContainerPrefix(blobPrefix)
        .build();
  }

  /**
   * Creates a new instance of {@link BlobContainerCopier} that uses the {@link
   * BlobContainerClientFactory} specified in the constructor as the destination storage account.
   *
   * @param clientFactory client factory of the source storage account and container.
   * @param pairs source and destination pairs to copy. If the destination is null or empty the
   *     source name will be used.
   * @return new instance of {@link BlobContainerClientFactory}.
   */
  public BlobContainerCopier createBlobContainerCopier(
      BlobContainerClientFactory clientFactory, List<BlobCopySourceDestinationPair> pairs) {
    return new BlobContainerCopierBuilder()
        .destinationClientFactory(blobContainerClientFactory)
        .sourceClientFactory(clientFactory)
        .sourceDestinationPairs(pairs)
        .build();
  }

  /**
   * Creates a new instance of {@link BlobContainerCopier} that uses the {@link
   * BlobContainerClientFactory} specified in the constructor as the destination storage account.
   *
   * @param clientFactory client factory of the source storage account and container.
   * @param sourceBlobName source blob name.
   * @param destinationBlobName destination blob name. If null or empty the source name will be
   *     used.
   * @return new instance of {@link BlobContainerClientFactory}.
   */
  public BlobContainerCopier createBlobContainerCopier(
      BlobContainerClientFactory clientFactory, String sourceBlobName, String destinationBlobName) {

    if (StringUtils.isBlank(sourceBlobName)) {
      throw new IllegalArgumentException("sourceBlobName is required. It's null or empty.");
    }

    return new BlobContainerCopierBuilder()
        .destinationClientFactory(blobContainerClientFactory)
        .sourceClientFactory(clientFactory)
        .sourceDestinationPairs(
            List.of(new BlobCopySourceDestinationPair(sourceBlobName, destinationBlobName)))
        .build();
  }

  /**
   * Creates a new instance of {@link BlobContainerCopier} that uses the {@link
   * BlobContainerClientFactory} specified in the constructor as the destination storage account.
   *
   * @param sourceUri source blob URI. The URL must include in the query string a SAS token with
   *     read access or must be a gs:// path
   * @param destinationBlobName destination blob name. If null or empty the source name will be
   *     used.
   * @return new instance of {@link BlobContainerClientFactory}.
   */
  public BlobContainerCopier createBlobContainerCopier(URI sourceUri, String destinationBlobName) {

    return new BlobContainerCopierBuilder()
        .destinationClientFactory(blobContainerClientFactory)
        .sourceBlobUrl(
            Objects.requireNonNull(
                    sourceUri,
                    "Source Blob URL is null. It must be a valid URL with read permissions")
                .toString())
        .destinationBlobName(destinationBlobName)
        .build();
  }

  /**
   * Deletes the specified blob in the container.
   *
   * @param blobName blob name to delete.
   * @return boolean that indicates the status of the delete operation
   */
  public boolean deleteBlob(String blobName) {
    try {
      blobContainerClientFactory.getBlobContainerClient().getBlobClient(blobName).delete();
      return true;
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
        return false;
      } else {
        throw new PdaoException("Error deleting file", e);
      }
    }
  }

  public boolean deleteBlobsWithPrefix(String blobName) {
    final String prefix;
    if (blobName.endsWith("/")) {
      prefix = blobName.substring(0, blobName.length() - 1);
    } else {
      prefix = blobName;
    }
    BlobContainerClient blobContainerClient = blobContainerClientFactory.getBlobContainerClient();

    try (Stream<BlobItem> blobsStream = blobContainerClient.listBlobs().stream()) {
      List<BlobItem> blobsToDelete =
          blobsStream
              .filter(blobItem -> blobItem.getName().startsWith(prefix))
              .collect(Collectors.toList());

      Collections.reverse(blobsToDelete);

      return blobsToDelete.stream()
          .map(item -> deleteBlob(item.getName()))
          .reduce(true, Boolean::logicalAnd);
    }
  }

  public boolean blobExists(String blobName) {
    return blobContainerClientFactory.getBlobContainerClient().getBlobClient(blobName).exists();
  }

  /**
   * Deletes the specified blob in the container. Logs on failure but does not throw error.
   *
   * @param blobName blob name to delete.
   */
  public void deleteBlobQuietFailure(String blobName) {
    try {
      boolean successDeleteParentBlob = deleteBlob(blobName);
      if (!successDeleteParentBlob) {
        logger.warn("Blob {} was not found, so could not be deleted.", blobName);
      }
    } catch (PdaoException e) {
      logger.warn("Could not delete the blob {}", blobName, e);
    }
  }

  /**
   * Returns the blob properties.
   *
   * @param blobName blob name.
   * @return Instance of {@link BlobProperties}
   */
  public BlobProperties getBlobProperties(String blobName) {
    return blobContainerClientFactory
        .getBlobContainerClient()
        .getBlobClient(blobName)
        .getProperties();
  }

  /**
   * Set a blob's md5 explicitly See <a
   * href="https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-properties-metadata-java">MS
   * Docs</a> for more information on API usage
   *
   * @param blobName blob name
   * @param contentMd5 the md5 value to set
   */
  public void setBlobMd5(String blobName, byte[] contentMd5) {
    BlobClient blobClient =
        blobContainerClientFactory.getBlobContainerClient().getBlobClient(blobName);

    BlobProperties properties = blobClient.getProperties();

    BlobHttpHeaders blobHeaders =
        new BlobHttpHeaders()
            .setCacheControl(properties.getCacheControl())
            .setContentDisposition(properties.getContentDisposition())
            .setContentEncoding(properties.getContentEncoding())
            .setContentLanguage(properties.getContentLanguage())
            .setContentType(properties.getContentType())
            .setContentMd5(contentMd5);

    blobClient.setHttpHeaders(blobHeaders);
  }

  /**
   * Creates a URL with a SAS token for a given blob.
   *
   * @param blobName blob name.
   * @param options sas token creation options.
   * @return Blob URL with a SAS token
   */
  public String createSasTokenUrlForBlob(String blobName, BlobSasTokenOptions options) {
    return blobContainerClientFactory.getBlobSasUrlFactory().createSasUrlForBlob(blobName, options);
  }

  /** Creates a the container in the storage account if it does not exist. */
  public void createContainerNameIfNotExists() {

    BlobContainerClient blobContainerClient = blobContainerClientFactory.getBlobContainerClient();

    if (!blobContainerClient.exists()) {
      blobContainerClient.create();
    }
  }
}
