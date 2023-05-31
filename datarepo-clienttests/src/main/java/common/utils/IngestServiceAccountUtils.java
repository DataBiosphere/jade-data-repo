package common.utils;

import bio.terra.datarepo.model.DatasetModel;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageRoles;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;
import scripts.utils.SAMUtils;

public final class IngestServiceAccountUtils {
  private static final Logger logger = LoggerFactory.getLogger(IngestServiceAccountUtils.class);

  /** Roles which must be held by a dataset's SA to facilitate an ingestion * */
  public static final List<Role> INGEST_ROLES =
      List.of(StorageRoles.objectViewer(), StorageRoles.legacyBucketReader());

  private IngestServiceAccountUtils() {}

  public static boolean isDedicatedServiceAccount(
      String serviceAccount, ServerSpecification server) {
    return StringUtils.isNotEmpty(serviceAccount)
        && !StringUtils.equalsIgnoreCase(serviceAccount, server.testRunnerServiceAccount.name);
  }

  public static void grantIngestBucketPermissionsToDedicatedSa(
      DatasetModel dataset, String ingestBucket, ServerSpecification server) throws IOException {
    String serviceAccount = dataset.getIngestServiceAccount();
    if (ingestBucket != null && isDedicatedServiceAccount(serviceAccount, server)) {
      for (var role : INGEST_ROLES) {
        addServiceAccountRoleToBucket(ingestBucket, serviceAccount, role);
      }
    }
  }

  static void addServiceAccountRoleToBucket(String bucket, String serviceAccount, Role role)
      throws IOException {
    logger.info("Granting role {} to {} on bucket {}", role, serviceAccount, bucket);
    Storage storage = StorageUtils.getStorage(GoogleCredentials.getApplicationDefault());
    Storage.BucketSourceOption[] options = new Storage.BucketSourceOption[0];
    Policy iamPolicy = storage.getIamPolicy(bucket, options);
    storage.setIamPolicy(
        bucket,
        iamPolicy.toBuilder().addIdentity(role, Identity.serviceAccount(serviceAccount)).build(),
        options);
  }

  public static void revokeIngestBucketPermissionsFromDedicatedSa(
      TestUserSpecification user,
      DatasetModel dataset,
      String ingestBucket,
      ServerSpecification server)
      throws Exception {
    String serviceAccount = dataset.getIngestServiceAccount();
    if (ingestBucket != null
        && IngestServiceAccountUtils.isDedicatedServiceAccount(serviceAccount, server)) {
      for (var role : IngestServiceAccountUtils.INGEST_ROLES) {
        IngestServiceAccountUtils.removeServiceAccountRoleFromBucket(
            ingestBucket, serviceAccount, role, dataset.getDataProject());
      }
      SAMUtils.deleteServiceAccountFromTerra(user, server, serviceAccount);
    }
  }

  public static void removeServiceAccountRoleFromBucket(
      String bucket, String serviceAccount, Role role, String userProject) throws IOException {
    logger.info("Revoking role {} from {} on bucket {}", role, serviceAccount, bucket);
    Storage storage = StorageUtils.getStorage(GoogleCredentials.getApplicationDefault());
    Storage.BucketSourceOption[] options = new Storage.BucketSourceOption[0];
    Policy iamPolicy = storage.getIamPolicy(bucket, options);
    storage.setIamPolicy(
        bucket,
        iamPolicy.toBuilder().removeIdentity(role, Identity.serviceAccount(serviceAccount)).build(),
        options);
  }
}
