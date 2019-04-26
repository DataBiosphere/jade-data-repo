package bio.terra.service;


public enum JobMapKeys {
    DESCRIPTION("description"),
    REQUEST("request"),
    RESPONSE("response"),
    STATUS_CODE("status_code"),
    USER_INFO("user_info"),
    STUDY_ID("studyId"),
    BQ_DATASET_INFO("bqDatasetInfo");

    private String keyName;

    JobMapKeys(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }
}





