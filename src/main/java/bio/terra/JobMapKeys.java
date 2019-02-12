package bio.terra;


public enum JobMapKeys {
    DESCRIPTION("description"),
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





