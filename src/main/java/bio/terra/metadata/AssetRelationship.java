package bio.terra.metadata;

import java.util.UUID;

public class AssetRelationship {
    private UUID id;
    private DrDatasetRelationship datasetRelationship;

    public UUID getId() {
        return id;
    }

    public AssetRelationship id(UUID id) {
        this.id = id;
        return this;
    }

    public DrDatasetRelationship getDatasetRelationship() {
        return datasetRelationship;
    }

    public AssetRelationship datasetRelationship(DrDatasetRelationship datasetRelationship) {
        this.datasetRelationship = datasetRelationship;
        return this;
    }
}
