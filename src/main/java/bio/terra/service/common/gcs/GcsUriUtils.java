package bio.terra.service.common.gcs;

import bio.terra.service.dataset.exception.InvalidUriException;
import com.google.cloud.storage.BlobInfo;
import org.apache.commons.lang3.StringUtils;

public final class GcsUriUtils {

  public static class GsUrlParts {
    private String bucket;
    private String path;
    private boolean isWildcard = false;

    public String getBucket() {
      return bucket;
    }

    public GsUrlParts bucket(String bucket) {
      this.bucket = bucket;
      return this;
    }

    public String getPath() {
      return path;
    }

    public GsUrlParts path(String path) {
      this.path = path;
      return this;
    }

    public boolean getIsWildcard() {
      return isWildcard;
    }

    public GsUrlParts isWildcard(boolean isWildcard) {
      this.isWildcard = isWildcard;
      return this;
    }
  }

  /**
   * Parse a Google Cloud Storage URI into its component pieces
   *
   * @param uri of type gs://<bucket_name>/<file_path_inside_bucket>
   * @return Object representing uri pieces
   */
  public static GsUrlParts parseBlobUri(String uri) {
    String protocol = "gs://";
    if (!StringUtils.startsWith(uri, protocol)) {
      throw new InvalidUriException("Ingest source is not a valid gs: URI: '" + uri + "'");
    }
    String noGsUri = StringUtils.substring(uri, protocol.length());
    if (noGsUri.length() < 4) {
      throw new InvalidUriException("Invalid bucket name in gs path: '" + uri + "'");
    }

    String bucket = StringUtils.substringBefore(noGsUri, "/");

    if (bucket.indexOf('*') > -1) {
      throw new InvalidUriException("Bucket wildcards are not supported: URI: '" + uri + "'");
    }

    String gsBucketPattern = "[a-z0-9_.\\-]{3,222}";
    if (!bucket.matches(gsBucketPattern)) {
      throw new InvalidUriException("Invalid bucket name in gs path: '" + uri + "'");
    }
    String[] bucketComponents = bucket.split("\\.");
    for (String component : bucketComponents) {
      if (component.length() > 63) {
        throw new InvalidUriException(
            "Component name '" + component + "' too long in gs path: '" + uri + "'");
      }
    }

    String path = StringUtils.substringAfter(noGsUri, "/");

    if (path.isEmpty()) {
      throw new InvalidUriException("Missing object name in gs path: '" + uri + "'");
    }

    int globIndex = path.indexOf('*');
    boolean isWildcard = globIndex > -1;
    if (isWildcard && path.lastIndexOf('*') != globIndex) {
      throw new InvalidUriException("Multi-wildcards are not supported: URI: '" + uri + "'");
    }

    return new GsUrlParts().bucket(bucket).path(path).isWildcard(isWildcard);
  }

  public static String getGsPathFromBlob(BlobInfo blob) {
    return getGsPathFromComponents(blob.getBucket(), blob.getName());
  }

  public static String getGsPathFromComponents(String bucket, String name) {
    return "gs://" + bucket + "/" + name;
  }

}
