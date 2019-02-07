package bio.terra;


public enum JobMapKeys {
    DESCRIPTION,
    RESPONSE,
    STATUS_CODE;

    String getStatus() {
        switch (this) {
            case DESCRIPTION:
                return "description";
            case RESPONSE:
                return "response";
            case STATUS_CODE:
                return "status code";
            default:
                throw new AssertionError("Unknown response" + this);
        }
    }
}





