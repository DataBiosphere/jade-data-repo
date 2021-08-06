package bio.terra.service.filedata.azure.util;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates SAS URLs for blobs from a Container URl with a SAS token. */
public class ContainerSasTokenSasUrlFactory implements BlobSasUrlFactory {
  private final BlobUrlParts blobContainerUrlParts;
  private static final Logger logger =
      LoggerFactory.getLogger(ContainerSasTokenSasUrlFactory.class);

  public ContainerSasTokenSasUrlFactory(BlobUrlParts blobContainerUrlParts) {
    this.blobContainerUrlParts = blobContainerUrlParts;
  }

  @Override
  public String createSasUrlForBlob(String blobName, BlobSasTokenOptions options) {

    // The SAS token of the blob is the same as the container
    // therefore the token options are ignored.
    // A potential future enhancement is to compare the BlobSasTokenOptions and the container SAS
    // token, confirm they match and throw an exception if not.
    logger.warn(
        "Using the container SAS token for blob:{}. BlobSasTokenOptions options were ignored.",
        blobName);

    return String.format(
        "https://%s/%s/%s?%s",
        blobContainerUrlParts.getHost(),
        blobContainerUrlParts.getBlobContainerName(),
        blobName,
        getSignature());
  }

  private String getSignature() {
    return new AzureSasCredential(blobContainerUrlParts.getCommonSasQueryParameters().encode())
        .getSignature();
  }
}
