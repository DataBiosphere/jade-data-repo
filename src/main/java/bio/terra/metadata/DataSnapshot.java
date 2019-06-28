package bio.terra.metadata;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class DataSnapshot {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;
    private List<Table> tables = Collections.emptyList();
    private List<DataSnapshotSource> dataSnapshotSources = Collections.emptyList();

    public UUID getId() {
        return id;
    }

    public DataSnapshot id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DataSnapshot name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DataSnapshot description(String description) {
        this.description = description;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public DataSnapshot createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public List<Table> getTables() {
        return tables;
    }

    public DataSnapshot datasetTables(List<Table> tables) {
        this.tables = tables;
        return this;
    }

    public List<DataSnapshotSource> getDataSnapshotSources() {
        return dataSnapshotSources;
    }

    public DataSnapshot datasetSources(List<DataSnapshotSource> dataSnapshotSources) {
        this.dataSnapshotSources = dataSnapshotSources;
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
