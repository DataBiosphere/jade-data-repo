package bio.terra.metadata;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DatasetSource {
    private UUID id;
    private Dataset dataset;
    private Study study;
    private AssetSpecification assetSpecification;
    private List<DatasetMapTable> datasetMapTables = Collections.emptyList();

    public Dataset getDataset() {
        return dataset;
    }

    public DatasetSource dataset(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }

    public Study getStudy() {
        return study;
    }

    public DatasetSource study(Study study) {
        this.study = study;
        return this;
    }

    public AssetSpecification getAssetSpecification() {
        return assetSpecification;
    }

    public DatasetSource assetSpecification(AssetSpecification assetSpecification) {
        this.assetSpecification = assetSpecification;
        return this;
    }

    public List<DatasetMapTable> getDatasetMapTables() {
        return datasetMapTables;
    }

    public DatasetSource datasetMapTables(List<DatasetMapTable> datasetMapTables) {
        this.datasetMapTables = datasetMapTables;
        return this;
    }

    public UUID getId() {

        return id;
    }

    public DatasetSource id(UUID id) {
        this.id = id;
        return this;
    }
}
