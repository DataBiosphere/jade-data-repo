package bio.terra.service.snapshot;

import bio.terra.model.CloudPlatform;
import java.util.List;
import java.util.UUID;

public class SnapshotProject {
  private UUID id;
  private String name;
  private UUID profileId;
  private String
      dataProject; // Project id of the snapshot data project--which is a string and feels like a
  // name
  private List<DatasetProject> sourceDatasetProjects;
  private CloudPlatform cloudPlatform;

  public UUID getId() {
    return id;
  }

  public SnapshotProject id(UUID id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public SnapshotProject name(String name) {
    this.name = name;
    return this;
  }

  public UUID getProfileId() {
    return profileId;
  }

  public SnapshotProject profileId(UUID profileId) {
    this.profileId = profileId;
    return this;
  }

  public String getDataProject() {
    return dataProject;
  }

  public SnapshotProject dataProject(String dataProject) {
    this.dataProject = dataProject;
    return this;
  }

  public List<DatasetProject> getSourceDatasetProjects() {
    return sourceDatasetProjects;
  }

  public DatasetProject getFirstSourceDatasetProject() {
    return sourceDatasetProjects.iterator().next();
  }

  public SnapshotProject sourceDatasetProjects(List<DatasetProject> sourceDatasetProjects) {
    this.sourceDatasetProjects = sourceDatasetProjects;
    return this;
  }

  public CloudPlatform getCloudPlatform() {
    return cloudPlatform;
  }

  public SnapshotProject cloudPlatform(CloudPlatform cloudPlatform) {
    this.cloudPlatform = cloudPlatform;
    return this;
  }
}
