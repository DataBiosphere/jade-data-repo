package bio.terra.datarepo.service.dataset;

import java.util.UUID;

public class DatasetDataProjectSummary {

  private UUID id;
  private UUID datasetId;
  private UUID projectResourceId;

  public UUID getId() {
    return id;
  }

  public DatasetDataProjectSummary id(UUID id) {
    this.id = id;
    return this;
  }

  public UUID getDatasetId() {
    return datasetId;
  }

  public DatasetDataProjectSummary datasetId(UUID datasetId) {
    this.datasetId = datasetId;
    return this;
  }

  public UUID getProjectResourceId() {
    return projectResourceId;
  }

  public DatasetDataProjectSummary projectResourceId(UUID projectResourceId) {
    this.projectResourceId = projectResourceId;
    return this;
  }
}
