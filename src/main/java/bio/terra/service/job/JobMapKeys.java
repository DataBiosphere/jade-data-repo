package bio.terra.service.job;

public enum JobMapKeys {
  // parameters for all flight types
  DESCRIPTION("description"),
  REQUEST("request"),
  REVERT_TO("revert_to"),
  RESPONSE("response"),
  STATUS_CODE("statusCode"),
  AUTH_USER_INFO("auth_user_info"),
  SUBJECT_ID("subjectId"),
  CLOUD_PLATFORM("cloudPlatform"),
  IAM_ACTION("iamAction"),
  IAM_RESOURCE_TYPE("iamResourceType"),
  IAM_RESOURCE_ID("iamResourceId"),
  PARENT_FLIGHT_ID("parentFlightId"),

  // parameters for specific flight types
  BILLING_ID("billingId"),
  DATASET_ID("datasetId"),
  SNAPSHOT_ID("snapshotId"),
  FILE_ID("fileId"),
  ASSET_ID("assetId"),
  TRANSACTION_ID("transactionId"),
  DELETE_CLOUD_RESOURCES("deleteCloudResources");

  private String keyName;

  JobMapKeys(String keyName) {
    this.keyName = keyName;
  }

  public String getKeyName() {
    return keyName;
  }
}
