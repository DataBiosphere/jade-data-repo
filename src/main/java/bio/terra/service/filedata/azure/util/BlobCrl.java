package bio.terra.service.filedata.azure.util;

import bio.terra.common.exception.PdaoException;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

/**
 * High-level API that enables copy, delete, and get blob properties operations on Azure Blob
 * storage.
 */
public class BlobCrl {

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
   * @param sourceUrl source blob URL. The URL must include in the query string a SAS token with
   *     read access.
   * @param destinationBlobName destination blob name. If null or empty the source name will be
   *     used.
   * @return new instance of {@link BlobContainerClientFactory}.
   */
  public BlobContainerCopier createBlobContainerCopier(URL sourceUrl, String destinationBlobName) {

    return new BlobContainerCopierBuilder()
        .destinationClientFactory(blobContainerClientFactory)
        .sourceBlobUrl(
            Objects.requireNonNull(
                    sourceUrl,
                    "Source Blob URL is null. It must be a valid URL with read permissions")
                .toString())
        .destinationBlobName(destinationBlobName)
        .build();
  }

  /**
   * Deletes the specified blob in the container.
   *
   * @param blobName blob name to delete.
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
