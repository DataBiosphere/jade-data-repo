package bio.terra.service.snapshot;

import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SnapshotSource {
    private UUID id;
    private Snapshot snapshot;
    private Dataset dataset;
    private AssetSpecification assetSpecification;
    private List<SnapshotMapTable> snapshotMapTables = Collections.emptyList();

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public SnapshotSource snapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public SnapshotSource dataset(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }

    public AssetSpecification getAssetSpecification() {
        return assetSpecification;
    }

    public SnapshotSource assetSpecification(AssetSpecification assetSpecification) {
        this.assetSpecification = assetSpecification;
        return this;
    }

    public List<SnapshotMapTable> getSnapshotMapTables() {
        return snapshotMapTables;
    }

    public SnapshotSource snapshotMapTables(List<SnapshotMapTable> snapshotMapTables) {
        this.snapshotMapTables = snapshotMapTables;
        return this;
    }

    public UUID getId() {

        return id;
    }

    public SnapshotSource id(UUID id) {
        this.id = id;
        return this;
    }
}
