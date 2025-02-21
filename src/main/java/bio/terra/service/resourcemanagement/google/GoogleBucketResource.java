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
  private String storageClassAsString;
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
  public String getStorageClassAsString() {
    return storageClassAsString;
  }

  // Required to avoid serialization/deserialization issues with Stairway
  public GoogleBucketResource storageClassAsString(String storageClassAsString) {
    this.storageClassAsString = storageClassAsString;
    return this;
  }

  @JsonIgnore
  public StorageClass getStorageClass() {
    return storageClassAsString == null ? null : StorageClass.valueOf(storageClassAsString);
  }

  @JsonIgnore
  public void setStorageClass(StorageClass storageClass) {
    this.storageClassAsString = storageClass == null ? null : storageClass.toString();
  }

  @JsonIgnore
  public GoogleBucketResource storageClass(StorageClass storageClass) {
    setStorageClass(storageClass);
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
  public void setTerminalStorageClass(StorageClass terminalStorageClass) {
    this.terminalStorageClassAsString =
        terminalStorageClass == null ? null : terminalStorageClass.toString();
  }

  @JsonIgnore
  public GoogleBucketResource terminalStorageClass(StorageClass terminalStorageClass) {
    setTerminalStorageClass(terminalStorageClass);
    return this;
  }

  @JsonIgnore
  public String projectIdForBucket() {
    return getProjectResource().getGoogleProjectId();
  }
}
