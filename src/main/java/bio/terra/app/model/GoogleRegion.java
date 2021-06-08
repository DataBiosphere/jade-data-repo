package bio.terra.app.model;

import bio.terra.service.dataset.StorageResource;

import java.util.List;
import java.util.Optional;

/**
 * Valid regions in Google.
 */
public enum GoogleRegion {
        ASIA_EAST2("asia-east2"),
        ASIA_EAST1("asia-east1", "asia-east2"),
        ASIA_NORTHEAST1("asia-northeast1"),
        ASIA_NORTHEAST2("asia-northeast2"),
        ASIA_NORTHEAST3("asia-northeast3"),
        ASIA_SOUTH1("asia-south1"),
        ASIA_SOUTHEAST2("asia-southeast2"),
        ASIA_SOUTHEAST1("asia-southeast1", "asia-southeast2"),
        AUSTRALIA_SOUTHEAST1("australia-southeast1"),
        EUROPE_CENTRAL2("europe-central2"),
        EUROPE_NORTH1("europe-north1"),
        EUROPE_WEST1("europe-west1", "europe-west2"),
        EUROPE_WEST2("europe-west2"),
        EUROPE_WEST3("europe-west3"),
        EUROPE_WEST4("europe-west4", "europe-west3"),
        EUROPE_WEST6("europe-west6"),
        NORTHAMERICA_NORTHEAST1("northamerica-northeast1"),
        SOUTHAMERICA_EAST1("southamerica-east1"),
        US_CENTRAL1("us-central1",  "us-east4"),
        US_EAST1("us-east1"),
        US_EAST4("us-east4"),
        US_WEST1("us-west1", "us-west2"),
        US_WEST2("us-west2"),
        US_WEST3("us-west3"),
        US_WEST4("us-west4"),
        US("us",  "us-east4", "us-east4");

    public static final GoogleRegion DEFAULT_GOOGLE_REGION = GoogleRegion.US_CENTRAL1;

    private final String value;
    private final String firestoreFallbackRegion;
    private final String bucketFallbackRegion;

    GoogleRegion(String value) {
        this(value, value, value);
    }

    GoogleRegion(String value, String firestoreFallbackRegion) {
        this(value, firestoreFallbackRegion, value);
    }

    GoogleRegion(String value, String firestoreFallbackRegion, String bucketFallbackRegion) {
        this.value = value;
        this.firestoreFallbackRegion = firestoreFallbackRegion;
        this.bucketFallbackRegion = bucketFallbackRegion;
    }

    public GoogleRegion getRegionOrFallbackFirestoreRegion() {
        return fromValue(firestoreFallbackRegion);
    }

    public GoogleRegion getRegionOrFallbackBucketRegion() {
        return fromValue(bucketFallbackRegion);
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

    public static boolean matchingRegionWithFallbacks(List<StorageResource> storage, GoogleRegion region) {
        GoogleRegion bqRegion = region;
        GoogleRegion firestoreRegion = region.getRegionOrFallbackFirestoreRegion();
        GoogleRegion bucketRegion = region.getRegionOrFallbackBucketRegion();
        boolean matching = true;
        for (StorageResource resource : storage) {
            if (resource.getCloudResource() == GoogleCloudResource.BIGQUERY) {
                if (resource.getRegion() != bqRegion) {
                    matching = false;
                }
            } else if (resource.getCloudResource() == GoogleCloudResource.FIRESTORE) {
                if (resource.getRegion() != firestoreRegion) {
                    matching = false;
                }
            } else if (resource.getCloudResource() == GoogleCloudResource.BUCKET) {
                if (resource.getRegion() != bucketRegion) {
                    matching = false;
                }
            }
        }
        return matching;
    }
}
