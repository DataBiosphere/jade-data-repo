package bio.terra.service.filedata.azure.util;

public interface BlobSasUrlFactory {
  String createSasUrlForBlob(String blobName, BlobSasTokenOptions options);
}
