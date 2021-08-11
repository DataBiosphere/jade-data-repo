package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.sas.BlobSasPermission;
import java.time.Duration;
import java.util.Objects;

public class BlobSasTokenOptions {

  private final Duration duration;
  private final BlobSasPermission sasPermissions;
  private final String contentDisposition;

  public BlobSasTokenOptions(
      Duration duration, BlobSasPermission sasPermissions, String contentDisposition) {

    this.duration = Objects.requireNonNull(duration, "Duration is required.");
    this.sasPermissions = Objects.requireNonNull(sasPermissions, "SAS permissions are required");
    this.contentDisposition = contentDisposition;
  }

  public Duration getDuration() {
    return duration;
  }

  public BlobSasPermission getSasPermissions() {
    return sasPermissions;
  }

  public String getContentDisposition() {
    return contentDisposition;
  }
}
