package bio.terra.metadata;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class Dataset {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;
    private List<Table> tables = Collections.emptyList();
    private List<DatasetSource> datasetSources = Collections.emptyList();
    private UUID profileId;

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

    public Instant getCreatedDate() {
        return createdDate;
    }

    public Dataset createdDate(Instant createdDate) {
        this.createdDate = createdDate;
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

    public UUID getProfileId() {
        return profileId;
    }

    public Dataset profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }
}
