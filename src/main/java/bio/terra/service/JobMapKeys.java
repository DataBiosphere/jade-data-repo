package bio.terra.service;


public enum JobMapKeys {
    DESCRIPTION("description"),
    REQUEST("request"),
    RESPONSE("response"),
    STATUS_CODE("status_code"),
    AUTH_USER_INFO("auth_user_info"),

    PATH_PARAMETERS("path_parameters"),
    DATASET_ID("datasetId"),
    SNAPSHOT_ID("snapshotId"),
    FILE_ID("fileId"),

    FLIGHT_CLASS("flight_class");

    private String keyName;

    JobMapKeys(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }
}





