package bio.terra.datarepo.service.dataset;

import bio.terra.datarepo.common.Relationship;
import java.util.UUID;

public class AssetRelationship {
  private UUID id;
  private Relationship datasetRelationship;

  public UUID getId() {
    return id;
  }

  public AssetRelationship id(UUID id) {
    this.id = id;
    return this;
  }

  public Relationship getDatasetRelationship() {
    return datasetRelationship;
  }

  public AssetRelationship datasetRelationship(Relationship datasetRelationship) {
    this.datasetRelationship = datasetRelationship;
    return this;
  }
}
