package bio.terra.integration;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public final class GcsFixtures {
  private static final Logger logger = LoggerFactory.getLogger(GcsFixtures.class);
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

  private GcsFixtures() {}

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

  static void addServiceAccountRoleToBucket(
      String bucket, String serviceAccount, Role role, String userProject) throws IOException {
    logger.info("Granting role {} to {} on bucket {}", role, serviceAccount, bucket);
    Storage storage = getStorage(GoogleCredentials.getApplicationDefault());
    Storage.BucketSourceOption[] options =
        Optional.ofNullable(userProject)
            .map(p -> new Storage.BucketSourceOption[] {Storage.BucketSourceOption.userProject(p)})
            .orElseGet(() -> new Storage.BucketSourceOption[0]);

    Policy iamPolicy = storage.getIamPolicy(bucket, options);
    storage.setIamPolicy(
        bucket,
        iamPolicy.toBuilder().addIdentity(role, Identity.serviceAccount(serviceAccount)).build(),
        options);
  }

  public static void removeServiceAccountRoleFromBucket(
      String bucket, String serviceAccount, Role role, String userProject) throws IOException {
    logger.info("Revoking role {} from {} on bucket {}", role, serviceAccount, bucket);
    Storage storage = getStorage(GoogleCredentials.getApplicationDefault());
    Storage.BucketSourceOption[] options =
        Optional.ofNullable(userProject)
            .map(p -> new Storage.BucketSourceOption[] {Storage.BucketSourceOption.userProject(p)})
            .orElseGet(() -> new Storage.BucketSourceOption[0]);
    Policy iamPolicy = storage.getIamPolicy(bucket, options);
    storage.setIamPolicy(
        bucket,
        iamPolicy.toBuilder().removeIdentity(role, Identity.serviceAccount(serviceAccount)).build(),
        options);
  }
}
