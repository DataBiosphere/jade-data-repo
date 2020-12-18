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

    // parameters for specific flight types
    DATASET_ID("datasetId"),
    SNAPSHOT_ID("snapshotId"),
    FILE_ID("fileId"),
    ASSET_ID("assetId");

    private String keyName;

    JobMapKeys(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }
}





