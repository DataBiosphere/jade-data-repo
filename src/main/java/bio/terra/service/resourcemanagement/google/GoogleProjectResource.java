package bio.terra.service.resourcemanagement.google;

import java.util.UUID;

public class GoogleProjectResource {
  private UUID id; // id of the project resource in the datarepo metadata
  private UUID profileId; // id of the associated billing profile
  private String googleProjectId; // google's user specified, globally unique id of the project
  private String googleProjectNumber; // google's auto generated numeric id of the project
  private String serviceAccount; // service account associated with this project

  // Default constructor for JSON serdes
  public GoogleProjectResource() {}

  public UUID getId() {
    return id;
  }

  public GoogleProjectResource id(UUID id) {
    this.id = id;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public GoogleProjectResource profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public String getGoogleProjectId() {
    return googleProjectId;
  }

  public GoogleProjectResource googleProjectId(String googleProjectId) {
    this.googleProjectId = googleProjectId;
    return this;
  }

  public String getGoogleProjectNumber() {
    return googleProjectNumber;
  }

  public GoogleProjectResource googleProjectNumber(String googleProjectNumber) {
    this.googleProjectNumber = googleProjectNumber;
    return this;
  }

  public String getServiceAccount() {
    return serviceAccount;
  }

  public GoogleProjectResource serviceAccount(String serviceAccount) {
    this.serviceAccount = serviceAccount;
    return this;
  }
}
