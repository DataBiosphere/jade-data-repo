package bio.terra.service;


public enum JobMapKeys {
    DESCRIPTION("description"),
    REQUEST("request"),
    PATH_PARAMETERS("path_parameters"),
    RESPONSE("response"),
    STATUS_CODE("status_code"),
    AUTH_USER_INFO("auth_user_info"),
    DATASET_ID("datasetId"),
    SNAPSHOT_ID("snapshotId"),
    FILE_ID("fileId");

    private String keyName;

    JobMapKeys(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }
}





