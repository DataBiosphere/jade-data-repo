package bio.terra.service.common;

public final class CommonMapKeys {
  private CommonMapKeys() {}

  private static final String PREFIX = "common-";

  public static final String DATASET_STORAGE_AUTH_INFO = PREFIX + "datasetStorageAuthInfo";
  public static final String SNAPSHOT_STORAGE_AUTH_INFO = PREFIX + "snapshotStorageAuthInfo";
  public static final String SHOULD_PERFORM_CONTAINER_ROLLBACK = PREFIX + "rollbackContainer";
  public static final String DATASET_STORAGE_ACCOUNT_RESOURCE =
      PREFIX + "datasetStorageAccountResource";
  public static final String SNAPSHOT_STORAGE_ACCOUNT_RESOURCE =
      PREFIX + "snapshotStorageAccountResource";

  public static final String COMPLETION_TO_FAILURE_EXCEPTION =
      PREFIX + "completionToFailureException";

  public static final String CREATED_AT = PREFIX + "createdAt";
}
