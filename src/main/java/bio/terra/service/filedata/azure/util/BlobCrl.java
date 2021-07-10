package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;

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
        .destinationClientFactory(this.blobContainerClientFactory)
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
        .destinationClientFactory(this.blobContainerClientFactory)
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
        .destinationClientFactory(this.blobContainerClientFactory)
        .sourceClientFactory(clientFactory)
        .sourceDestinationPairs(
            Arrays.asList(new BlobCopySourceDestinationPair(sourceBlobName, destinationBlobName)))
        .build();
  }

  /**
   * Creates a new instance of {@link BlobContainerCopier} that uses the {@link
   * BlobContainerClientFactory} specified in the constructor as the destination storage account.
   *
   * @param clientFactory client factory of the source storage account and container.
   * @param sourceUrl source blob URL. The URL must include in the query string a SAS token with
   *     read access.
   * @param destinationBlobName destination blob name. If null or empty the source name will be
   *     used.
   * @return new instance of {@link BlobContainerClientFactory}.
   */
  public BlobContainerCopier createBlobContainerCopier(
      BlobContainerClientFactory clientFactory, URL sourceUrl, String destinationBlobName) {

    return new BlobContainerCopierBuilder()
        .destinationClientFactory(this.blobContainerClientFactory)
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
  public void deleteBlob(String blobName) {
    this.blobContainerClientFactory.getBlobContainerClient().getBlobClient(blobName).delete();
  }

  /**
   * Returns the blob properties.
   *
   * @param blobName blob name.
   * @return Instance of {@link BlobProperties}
   */
  public BlobProperties getBlobProperties(String blobName) {
    return this.blobContainerClientFactory
        .getBlobContainerClient()
        .getBlobClient(blobName)
        .getProperties();
  }

  /** Creates a the container in the storage account if it does not exist. */
  public void createContainerNameIfNotExists() {

    BlobContainerClient blobContainerClient =
        this.blobContainerClientFactory.getBlobContainerClient();

    if (!blobContainerClient.exists()) {
      blobContainerClient.create();
    }
  }
}
