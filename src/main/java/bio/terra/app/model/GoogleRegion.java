package bio.terra.app.model;

import java.util.Optional;

/**
 * Valid regions in Google.
 */
public enum GoogleRegion {
        ASIA_EAST2("asia-east2"),
        ASIA_EAST1("asia-east1", GoogleRegion.ASIA_EAST2, null),
        ASIA_NORTHEAST1("asia-northeast1"),
        ASIA_NORTHEAST2("asia-northeast2"),
        ASIA_NORTHEAST3("asia-northeast3"),
        ASIA_SOUTH1("asia-south1"),
        ASIA_SOUTHEAST2("asia-southeast2"),
        ASIA_SOUTHEAST1("asia-southeast1", GoogleRegion.ASIA_SOUTHEAST2, null),
        AUSTRALIA_SOUTHEAST1("australia-southeast1"),
        EUROPE_CENTRAL2("europe-central2"),
        EUROPE_NORTH1("europe-north1"),
        EUROPE_WEST2("europe-west2"),
        EUROPE_WEST3("europe-west3"),
        EUROPE_WEST6("europe-west6"),
        EUROPE_WEST1("europe-west1", GoogleRegion.EUROPE_WEST2, null),
        EUROPE_WEST4("europe-west4", GoogleRegion.EUROPE_WEST3, null),
        NORTHAMERICA_NORTHEAST1("northamerica-northeast1"),
        SOUTHAMERICA_EAST1("southamerica-east1"),
        US_EAST1("us-east1"),
        US_EAST4("us-east4"),
        US_WEST2("us-west2"),
        US_WEST3("us-west3"),
        US_WEST4("us-west4"),
        US_WEST1("us-west1", GoogleRegion.US_WEST2, null),
        US_CENTRAL1("us-central1",  GoogleRegion.US_EAST4, null),
        US("us",  GoogleRegion.US_EAST4, GoogleRegion.US_EAST4);

    public static final GoogleRegion DEFAULT_GOOGLE_REGION = GoogleRegion.US_EAST4;

    private final String value;
    private final GoogleRegion firestoreFallbackRegion;
    private final GoogleRegion bucketFallbackRegion;

    GoogleRegion(String value) {
        this(value, null, null);
    }

    GoogleRegion(String value, GoogleRegion firestoreFallbackRegion, GoogleRegion bucketFallbackRegion) {
        this.value = value;
        this.firestoreFallbackRegion = firestoreFallbackRegion;
        this.bucketFallbackRegion = bucketFallbackRegion;
    }

    public GoogleRegion getFirestoreRegion() {
        if (firestoreFallbackRegion != null) {
            return firestoreFallbackRegion;
        }
        return this;
    }

    public GoogleRegion getBucketRegion() {
        if (bucketFallbackRegion != null) {
            return bucketFallbackRegion;
        }
        return this;
    }

    public String toString() {
        return value;
    }

    public static GoogleRegion fromValue(String text) {
        for (GoogleRegion region : GoogleRegion.values()) {
            if (region.value.equalsIgnoreCase(text)) {
                return region;
            }
        }
        return null;
    }

    public static GoogleRegion fromValueWithDefault(String text) {
        return Optional.ofNullable(GoogleRegion.fromValue(text)).orElse(GoogleRegion.DEFAULT_GOOGLE_REGION);
    }
}
