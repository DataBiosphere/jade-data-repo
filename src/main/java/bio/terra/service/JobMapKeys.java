package bio.terra.service;


public enum JobMapKeys {
    DESCRIPTION("description"),
    REQUEST("request"),
    RESPONSE("response"),
    STATUS_CODE("status_code");

    private String keyName;

    JobMapKeys(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }
}





