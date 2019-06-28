package bio.terra.metadata;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DataSnapshotSource {
    private UUID id;
    private DataSnapshot dataSnapshot;
    private Study study;
    private AssetSpecification assetSpecification;
    private List<DataSnapshotMapTable> dataSnapshotMapTables = Collections.emptyList();

    public DataSnapshot getDataSnapshot() {
        return dataSnapshot;
    }

    public DataSnapshotSource dataset(DataSnapshot dataSnapshot) {
        this.dataSnapshot = dataSnapshot;
        return this;
    }

    public Study getStudy() {
        return study;
    }

    public DataSnapshotSource study(Study study) {
        this.study = study;
        return this;
    }

    public AssetSpecification getAssetSpecification() {
        return assetSpecification;
    }

    public DataSnapshotSource assetSpecification(AssetSpecification assetSpecification) {
        this.assetSpecification = assetSpecification;
        return this;
    }

    public List<DataSnapshotMapTable> getDataSnapshotMapTables() {
        return dataSnapshotMapTables;
    }

    public DataSnapshotSource datasetMapTables(List<DataSnapshotMapTable> dataSnapshotMapTables) {
        this.dataSnapshotMapTables = dataSnapshotMapTables;
        return this;
    }

    public UUID getId() {

        return id;
    }

    public DataSnapshotSource id(UUID id) {
        this.id = id;
        return this;
    }
}
