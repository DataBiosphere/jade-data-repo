package bio.terra.service.job;

public enum JobMapKeys {
  // parameters for all flight types
  DESCRIPTION("description"),
  REQUEST("request"),
  REVERT_TO("revert_to"),
  RESPONSE("response"),
  STATUS_CODE("status_code"),
  AUTH_USER_INFO("auth_user_info"),
  SUBJECT_ID("subjectId"),
  CLOUD_PLATFORM("cloudPlatform"),
  READ_JOB_ACLS("readJobAcls"),

  // parameters for specific flight types
  BILLING_ID("billingId"),
  DATASET_ID("datasetId"),
  SNAPSHOT_ID("snapshotId"),
  FILE_ID("fileId"),
  ASSET_ID("assetId"),
  TRANSACTION_ID("transactionId");

  private String keyName;

  JobMapKeys(String keyName) {
    this.keyName = keyName;
  }

  public String getKeyName() {
    return keyName;
  }
}
