package bio.terra.app.model;

import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.dataset.StorageResource;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Valid regions in Google. */
public enum GoogleRegion implements CloudRegion {
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
  US_CENTRAL1("us-central1", "us-east4"),
  US_EAST1("us-east1"),
  US_EAST4("us-east4"),
  US_WEST1("us-west1", "us-west2"),
  US_WEST2("us-west2"),
  US_WEST3("us-west3"),
  US_WEST4("us-west4"),
  US("us", "us-east4", "us-east4", true);

  public static final GoogleRegion DEFAULT_GOOGLE_REGION = GoogleRegion.US_CENTRAL1;

  public static final List<String> SUPPORTED_REGIONS =
      Arrays.stream(GoogleRegion.values())
          .map(GoogleRegion::getValue)
          .collect(Collectors.toUnmodifiableList());

  private final String value;
  private final String firestoreFallbackRegion;
  private final String bucketFallbackRegion;
  private final boolean isMultiRegional;

  GoogleRegion(String value) {
    this(value, value, value, false);
  }

  GoogleRegion(String value, String firestoreFallbackRegion) {
    this(value, firestoreFallbackRegion, value, false);
  }

  GoogleRegion(
      String value,
      String firestoreFallbackRegion,
      String bucketFallbackRegion,
      boolean isMultiRegional) {
    this.value = value;
    this.firestoreFallbackRegion = firestoreFallbackRegion;
    this.bucketFallbackRegion = bucketFallbackRegion;
    this.isMultiRegional = isMultiRegional;
  }

  public GoogleRegion getRegionOrFallbackFirestoreRegion() {
    return fromValue(firestoreFallbackRegion);
  }

  public GoogleRegion getRegionOrFallbackBucketRegion() {
    return fromValue(bucketFallbackRegion);
  }

  @Override
  public String getValue() {
    return value;
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
    return Optional.ofNullable(GoogleRegion.fromValue(text))
        .orElse(GoogleRegion.DEFAULT_GOOGLE_REGION);
  }

  public static boolean matchingRegionWithFallbacks(
      List<? extends StorageResource<?, ?>> storage, GoogleRegion region) {
    var gcpStorage =
        storage.stream()
            .filter(s -> s.getCloudPlatform() == CloudPlatform.GCP)
            .map(s -> (GoogleStorageResource) s)
            .collect(Collectors.toList());
    for (GoogleStorageResource resource : gcpStorage) {
      switch (resource.getCloudResource()) {
        case BIGQUERY:
          if (resource.getRegion() != region) {
            return false;
          }
          break;
        case FIRESTORE:
          if (resource.getRegion() != region.getRegionOrFallbackFirestoreRegion()) {
            return false;
          }
          break;
        case BUCKET:
          if (resource.getRegion() != region.getRegionOrFallbackBucketRegion()) {
            return false;
          }
      }
    }
    return true;
  }

  public boolean isMultiRegional() {
    return isMultiRegional;
  }
}
