package bio.terra.stairway;


public enum FlightStatusEnum {
    RESPONSE,
    STATUS_CODE;

    String getStatus() {
        switch (this) {
            case RESPONSE:
                return "response";
            case STATUS_CODE:
                return "status code";
            default:
                throw new AssertionError("Unknown response" + this);
        }
    }
}





