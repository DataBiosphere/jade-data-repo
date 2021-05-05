package bio.terra.app.model;

/**
 * Valid regions in Google.
 */
public enum GoogleRegion {
    ASIA_EAST1("asia-east1"),
    ASIA_EAST2("asia-east2"),
    ASIA_NORTHEAST1("asia-northeast1"),
    ASIA_NORTHEAST2("asia-northeast2"),
    ASIA_SOUTH1("asia-south1"),
    ASIA_SOUTHEAST1("asia-southeast1"),
    AUSTRALIA_SOUTHEAST1("australia-southeast1"),
    EUROPE_NORTH1("europe-north1"),
    EUROPE_WEST1("europe-west1"),
    EUROPE_WEST2("europe-west2"),
    EUROPE_WEST3("europe-west3"),
    EUROPE_WEST4("europe-west4"),
    EUROPE_WEST6("europe-west6"),
    NORTHAMERICA_NORTHEAST1("northamerica-northeast1"),
    SOUTHAMERICA_EAST1("southamerica-east1"),
    US_CENTRAL1("us-central1"),
    US_EAST1("us-east1"),
    US_EAST4("us-east4"),
    US_WEST1("us-west1"),
    US_WEST2("us-west2");

    private static GoogleRegion defaultGoogleRegion = GoogleRegion.US_CENTRAL1;

    private String value;

    GoogleRegion(String value) {
        this.value = value;
    }

    public static GoogleRegion getDefaultGoogleRegion() {
        return GoogleRegion.defaultGoogleRegion;
    }

    public String toString() {
        return String.valueOf(value);
    }

    public static GoogleRegion fromValue(String text) {
        for (GoogleRegion b : GoogleRegion.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
