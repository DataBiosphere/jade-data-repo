package bio.terra.service.dataset;

import java.util.UUID;

public class AssetRelationship {
    private UUID id;
    private DatasetRelationship datasetRelationship;

    public UUID getId() {
        return id;
    }

    public AssetRelationship id(UUID id) {
        this.id = id;
        return this;
    }

    public DatasetRelationship getDatasetRelationship() {
        return datasetRelationship;
    }

    public AssetRelationship datasetRelationship(DatasetRelationship datasetRelationship) {
        this.datasetRelationship = datasetRelationship;
        return this;
    }
}
