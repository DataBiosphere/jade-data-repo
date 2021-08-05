package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.sas.BlobSasPermission;
import java.time.Duration;
import java.util.Objects;

public class BlobSasTokenOptions {

  private final Duration expiration;
  private final BlobSasPermission sasPermissions;
  private final String contentDisposition;

  public BlobSasTokenOptions(
      Duration expiration, BlobSasPermission sasPermissions, String contentDisposition) {

    this.expiration = Objects.requireNonNull(expiration, "Expiration is required.");
    this.sasPermissions = Objects.requireNonNull(sasPermissions, "SAS permissions are required");
    this.contentDisposition = contentDisposition;
  }

  public Duration getExpiration() {
    return expiration;
  }

  public BlobSasPermission getSasPermissions() {
    return sasPermissions;
  }

  public String getContentDisposition() {
    return contentDisposition;
  }
}
