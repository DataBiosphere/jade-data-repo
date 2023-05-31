package common.utils;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import runner.config.ServiceAccountSpecification;

public class StorageUtils {
  private static final Logger logger = LoggerFactory.getLogger(StorageUtils.class);
  private static final int connectTimeoutSeconds = 100;
  private static final int readTimeoutSeconds = 100;
  private static final RetrySettings retrySettings =
      RetrySettings.newBuilder()
          .setInitialRetryDelay(Duration.ofSeconds(1))
          .setMaxRetryDelay(Duration.ofSeconds(32))
          .setRetryDelayMultiplier(2.0)
          .setTotalTimeout(Duration.ofMinutes(5))
          .setInitialRpcTimeout(Duration.ofSeconds(50))
          .setRpcTimeoutMultiplier(1.0)
          .setMaxRpcTimeout(Duration.ofSeconds(50))
          .build();

  private StorageUtils() {}

  public static Storage getStorage(Credentials credentials) {
    HttpTransportOptions transportOptions =
        StorageOptions.getDefaultHttpTransportOptions().toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
            .setReadTimeout(readTimeoutSeconds * 1000)
            .build();
    return StorageOptions.newBuilder()
        .setTransportOptions(transportOptions)
        .setCredentials(credentials)
        .setRetrySettings(retrySettings)
        .build()
        .getService();
  }

  /**
   * Build a Google Storage client object with credentials for the given service account. The client
   * object is newly created on each call to this method; it is not cached.
   */
  public static Storage getClientForServiceAccount(ServiceAccountSpecification serviceAccount)
      throws Exception {
    logger.debug(
        "Fetching credentials and building Storage client object for service account: {}",
        serviceAccount.name);

    GoogleCredentials serviceAccountCredentials =
        AuthenticationUtils.getServiceAccountCredential(serviceAccount);
    StorageOptions storageOptions =
        StorageOptions.newBuilder().setCredentials(serviceAccountCredentials).build();
    Storage storageClient = storageOptions.getService();

    return storageClient;
  }

  /**
   * Write the contents of a byte array to a file in the given GCS bucket.
   *
   * @param byteArray the bytes to write
   * @param fileName the name of the file
   * @param bucketName the bucket to write to
   * @return the created BlobId
   */
  public static BlobId writeBytesToFile(
      Storage storageClient, String bucketName, String fileName, byte[] byteArray) {
    BlobInfo blobToCreate = BlobInfo.newBuilder(bucketName, fileName).build();
    Blob createdBlob = storageClient.create(blobToCreate, byteArray);

    return createdBlob.getBlobId();
  }

  /** Convert a given BlobId to a gs:// path. Does not check for existence. */
  public static String blobIdToGSPath(BlobId blobId) {
    return String.format("gs://%s/%s", blobId.getBucket(), blobId.getName());
  }

  /** Delete all files in the given list. */
  public static void deleteFiles(Storage storageClient, List<BlobId> files) {
    for (BlobId file : files) {
      Blob blob = storageClient.get(file);
      if (blob == null) {
        logger.debug(
            "Blob not found: bucket = {}, file name = {}", file.getBucket(), file.getName());
      } else {
        logger.debug(
            "Deleting blob: bucket = {}, file name = {}", file.getBucket(), file.getName());
        blob.delete();
      }
    }
  }
}
