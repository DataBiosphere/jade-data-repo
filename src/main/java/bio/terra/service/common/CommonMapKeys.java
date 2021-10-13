package bio.terra.service.common;

public final class CommonMapKeys {
  private CommonMapKeys() {}

  private static final String PREFIX = "common-";

  public static final String DATASET_STORAGE_AUTH_INFO = PREFIX + "datasetStorageAuthInfo";
  public static final String SNAPSHOT_STORAGE_AUTH_INFO = PREFIX + "snapshotStorageAuthInfo";
  public static final String DATASET_STORAGE_ACCOUNT_RESOURCE =
      PREFIX + "datasetStorageAccountResource";
  public static final String SNAPSHOT_STORAGE_ACCOUNT_RESOURCE =
      PREFIX + "snapshotStorageAccountResource";
}
