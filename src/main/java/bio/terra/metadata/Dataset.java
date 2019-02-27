package bio.terra.metadata;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class Dataset {
    private UUID id;
    private String name;
    private String description;
    private Timestamp createdDate;
    private List<Table> tables = Collections.emptyList();
    private List<DatasetSource> datasetSources = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public Dataset id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Dataset name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Dataset description(String description) {
        this.description = description;
        return this;
    }

    public Timestamp getCreatedDate() {
        return new Timestamp(createdDate.getTime());
    }

    public Dataset createdDate(Timestamp createdDate) {
        this.createdDate = new Timestamp(createdDate.getTime());
        return this;
    }

    public List<Table> getTables() {
        return tables;
    }

    public Dataset datasetTables(List<Table> tables) {
        this.tables = tables;
        return this;
    }

    public List<DatasetSource> getDatasetSources() {
        return datasetSources;
    }

    public Dataset datasetSources(List<DatasetSource> datasetSources) {
        this.datasetSources = datasetSources;
        return this;
    }

    public Optional<Table> getTableById(UUID id) {
        for (Table tryTable : getTables()) {
            if (tryTable.getId().equals(id)) {
                return Optional.of(tryTable);
            }
        }
        return Optional.empty();
    }
}
