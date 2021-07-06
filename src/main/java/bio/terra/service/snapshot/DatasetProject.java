package bio.terra.service.snapshot;

import java.util.UUID;

public class DatasetProject {
  private UUID id;
  private String name;
  private UUID profileId;
  private String
      dataProject; // Project id of the snapshot data project--which is a string and feels like a
  // name

  public UUID getId() {
    return id;
  }

  public DatasetProject id(UUID id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public DatasetProject name(String name) {
    this.name = name;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public DatasetProject profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public String getDataProject() {
    return dataProject;
  }

  public DatasetProject dataProject(String dataProject) {
    this.dataProject = dataProject;
    return this;
  }
}
