package bio.terra.metadata;

import java.time.Instant;
import java.util.UUID;

public class DataSnapshotSummary {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;

    public UUID getId() {
        return id;
    }

    public DataSnapshotSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DataSnapshotSummary name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DataSnapshotSummary description(String description) {
        this.description = description;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public DataSnapshotSummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }
}
