package bio.terra.service.common.gcs;

import static bio.terra.service.filedata.google.gcs.GcsConstants.USER_PROJECT_QUERY_PARAM;

import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public final class GcsUriUtils {
  private static final Pattern GS_BUCKET_PATTERN = Pattern.compile("[a-z0-9_.\\-]{3,222}");

  /**
   * Parse a Google Cloud Storage URI into its component pieces
   *
   * @param uri of type gs://[bucket_name]/[file_path_inside_bucket]
   * @return Object representing uri pieces
   */
  public static BlobId parseBlobUri(String uri) throws IllegalArgumentException {
    BlobId blobId = fromGsUtilUri(uri);
    validateBlobUri(blobId, uri);
    return blobId;
  }

  public static void validateBlobUri(String uri) throws IllegalArgumentException {
    validateBlobUri(fromGsUtilUri(uri), uri);
  }

  private static BlobId fromGsUtilUri(String gsUtilUri) {
    if (!Pattern.matches("gs://.*/.*", gsUtilUri)) {
      throw new IllegalArgumentException(
          gsUtilUri + " is not a valid gsutil URI (i.e. \"gs://bucket/blob\")");
    }
    int blobNameStartIndex = gsUtilUri.indexOf('/', 5);
    String bucketName = gsUtilUri.substring(5, blobNameStartIndex);
    String blobName = gsUtilUri.substring(blobNameStartIndex + 1);

    return BlobId.of(bucketName, blobName);
  }

  private static void validateBlobUri(BlobId blobId, String uri) throws IllegalArgumentException {

    String bucket = blobId.getBucket();

    if (bucket.indexOf('*') > -1) {
      throw new IllegalArgumentException("Bucket wildcards are not supported: URI: '" + uri + "'");
    }

    if (!GS_BUCKET_PATTERN.matcher(bucket).matches()) {
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

    // check for multi wildcards
    int globIndex = path.indexOf('*');
    boolean isWildcard = globIndex > -1;
    if (isWildcard && path.lastIndexOf('*') != globIndex) {
      throw new IllegalArgumentException("Multi-wildcards are not supported: URI: '" + uri + "'");
    }
  }

  public static String getGsPathFromBlob(BlobInfo blob) {
    return getGsPathFromComponents(blob.getBucket(), blob.getName());
  }

  public static String getGsPathFromBlob(BlobId blob) {
    return getGsPathFromComponents(blob.getBucket(), blob.getName());
  }

  public static String getGsPathFromComponents(String bucket, String name) {
    return "gs://" + bucket + "/" + name;
  }

  public static BlobId getBlobForFlight(String bucket, String name, String flightId) {
    return parseBlobUri(getPathForFlight(bucket, name, flightId));
  }

  public static String getPathForFlight(String bucket, String name, String flightId) {
    return getGsPathFromComponents(bucket, String.format("%s/%s", flightId, name));
  }

  public static String getControlPath(
      String path, GoogleBucketResource bucketResource, String flightId) {
    BlobId controlBlob = GcsUriUtils.parseBlobUri(path);
    String newPath =
        GcsUriUtils.getPathForFlight(bucketResource.getName(), controlBlob.getName(), flightId);
    int lastWildcard = newPath.lastIndexOf("*");
    return lastWildcard >= 0 ? newPath.substring(0, lastWildcard + 1) : newPath;
  }

  public static String makeHttpsFromGs(String gspath, String userProject) {
    BlobId locator = GcsUriUtils.parseBlobUri(gspath);
    String gsBucket = locator.getBucket();
    String gsPath = locator.getName();
    String userProjectParam;
    if (userProject != null) {
      userProjectParam = "%s=%s&".formatted(USER_PROJECT_QUERY_PARAM, userProject);
    } else {
      userProjectParam = "";
    }
    String encodedPath =
        URLEncoder.encode(gsPath, StandardCharsets.UTF_8)
            // Google does not recognize the + characters that are produced from spaces by the
            // URLEncoder.encode method. As a result, these must be converted to %2B.
            .replaceAll("\\+", "%20");
    return "https://www.googleapis.com/storage/v1/b/%s/o/%s?%salt=media"
        .formatted(gsBucket, encodedPath, userProjectParam);
  }

  /**
   * Performs rudimentary test on a potential gcs uri to see if it might be valid (note: does not
   * confirm the validity of the gcs path)
   *
   * @param uri A path to evaluate
   * @return A boolean true is uri might be a valid gs path
   */
  public static boolean isGsUri(String uri) {
    return uri != null && uri.startsWith("gs://");
  }
}
