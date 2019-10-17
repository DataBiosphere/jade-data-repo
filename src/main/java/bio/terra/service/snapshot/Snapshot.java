package bio.terra.service.snapshot;

import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.common.Table;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class Snapshot implements FSContainerInterface {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;
    private List<SnapshotTable> tables = Collections.emptyList();
    private List<SnapshotSource> snapshotSources = Collections.emptyList();
    private UUID profileId;
    private SnapshotDataProject dataProject = new SnapshotDataProject();

    public UUID getId() {
        return id;
    }

    public Snapshot id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Snapshot name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Snapshot description(String description) {
        this.description = description;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public Snapshot createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public List<SnapshotTable> getTables() {
        return tables;
    }

    public Snapshot snapshotTables(List<SnapshotTable> tables) {
        this.tables = tables;
        return this;
    }

    public List<SnapshotSource> getSnapshotSources() {
        return snapshotSources;
    }

    public Snapshot snapshotSources(List<SnapshotSource> snapshotSources) {
        this.snapshotSources = snapshotSources;
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

    public Snapshot profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public SnapshotDataProject getDataProject() {
        return dataProject;
    }

    public Snapshot dataProject(SnapshotDataProject dataProject) {
        this.dataProject = dataProject;
        return this;
    }

    public String getDataProjectId() {
        return dataProject.getGoogleProjectId();
    }

    public Snapshot dataProjectId(String projectId) {
        dataProject.googleProjectId(projectId);
        return this;
    }
}
