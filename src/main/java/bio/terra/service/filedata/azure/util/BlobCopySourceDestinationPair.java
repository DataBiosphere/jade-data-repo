package bio.terra.service.filedata.azure.util;

/** Represents a pair/tuple of source and destination blob names. */
public final class BlobCopySourceDestinationPair {
  private final String sourceBlobName;
  private final String destinationBlobName;

  public BlobCopySourceDestinationPair(String sourceBlobName, String destinationBlobName) {
    this.sourceBlobName = sourceBlobName;
    this.destinationBlobName = destinationBlobName;
  }

  public String getSourceBlobName() {
    return sourceBlobName;
  }

  public String getDestinationBlobName() {
    return destinationBlobName;
  }
}
