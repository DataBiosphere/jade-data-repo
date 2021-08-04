package bio.terra.service.common.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;

public final class GcsUriUtils {
  private static final String GS_BUCKET_PATTERN = "[a-z0-9_.\\-]{3,222}";

  /**
   * Parse a Google Cloud Storage URI into its component pieces
   *
   * @param uri of type gs://<bucket_name>/<file_path_inside_bucket>
   * @return Object representing uri pieces
   */
  public static BlobId parseBlobUri(String uri) {
    BlobId blobId = BlobId.fromGsUtilUri(uri);
    validateBlobUri(blobId, uri);
    return blobId;
  }

  public static void validateBlobUri(String uri) throws IllegalArgumentException {
    validateBlobUri(BlobId.fromGsUtilUri(uri), uri);
  }

  private static void validateBlobUri(BlobId blobId, String uri) throws IllegalArgumentException {

    String bucket = blobId.getBucket();

    if (bucket.indexOf('*') > -1) {
      throw new IllegalArgumentException("Bucket wildcards are not supported: URI: '" + uri + "'");
    }

    if (!bucket.matches(GS_BUCKET_PATTERN)) {
      throw new IllegalArgumentException("Invalid bucket name in gs path: '" + uri + "'");
    }
    String[] bucketComponents = bucket.split("\\.");
    for (String component : bucketComponents) {
      if (component.length() > 63) {
        throw new IllegalArgumentException(
            "Component name '" + component + "' too long in gs path: '" + uri + "'");
      }
    }

    String path = blobId.getName();

    if (path.isEmpty()) {
      throw new IllegalArgumentException("Missing object name in gs path: '" + uri + "'");
    }
  }

  public static String getGsPathFromBlob(BlobInfo blob) {
    return getGsPathFromComponents(blob.getBucket(), blob.getName());
  }

  public static String getGsPathFromComponents(String bucket, String name) {
    return "gs://" + bucket + "/" + name;
  }
}
