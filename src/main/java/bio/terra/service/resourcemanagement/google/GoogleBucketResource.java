package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleRegion;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.storage.StorageClass;
import java.util.UUID;

public class GoogleBucketResource {
  private UUID resourceId;
  private String flightId;
  private UUID profileId;
  private GoogleProjectResource projectResource;
  private String name;
  private GoogleRegion region;
  private boolean autoclassEnabled;
  private String terminalStorageClassAsString;

  public GoogleBucketResource() {}

  public UUID getResourceId() {
    return resourceId;
  }

  public GoogleBucketResource resourceId(UUID resourceId) {
    this.resourceId = resourceId;
    return this;
  }

  public String getFlightId() {
    return flightId;
  }

  public GoogleBucketResource flightId(String flightId) {
    this.flightId = flightId;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public GoogleBucketResource profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public GoogleProjectResource getProjectResource() {
    return projectResource;
  }

  public GoogleBucketResource projectResource(GoogleProjectResource projectResource) {
    this.projectResource = projectResource;
    return this;
  }

  public String getName() {
    return name;
  }

  public GoogleBucketResource name(String name) {
    this.name = name;
    return this;
  }

  public GoogleRegion getRegion() {
    return region;
  }

  public GoogleBucketResource region(GoogleRegion region) {
    this.region = region;
    return this;
  }

  public boolean getAutoclassEnabled() {
    return autoclassEnabled;
  }

  public GoogleBucketResource autoclassEnabled(boolean autoclassEnabled) {
    this.autoclassEnabled = autoclassEnabled;
    return this;
  }

  // Required to avoid serialization/deserialization issues with Stairway
  public String getTerminalStorageClassAsString() {
    return terminalStorageClassAsString;
  }

  // Required to avoid serialization/deserialization issues with Stairway
  public GoogleBucketResource terminalStorageClassAsString(String terminalStorageClassAsString) {
    this.terminalStorageClassAsString = terminalStorageClassAsString;
    return this;
  }

  @JsonIgnore
  public StorageClass getTerminalStorageClass() {
    return terminalStorageClassAsString == null
        ? null
        : StorageClass.valueOf(terminalStorageClassAsString);
  }

  @JsonIgnore
  public GoogleBucketResource terminalStorageClass(StorageClass terminalStorageClass) {
    this.terminalStorageClassAsString =
        terminalStorageClass == null ? null : terminalStorageClass.toString();
    return this;
  }

  @JsonIgnore
  public String projectIdForBucket() {
    return getProjectResource().getGoogleProjectId();
  }
}
