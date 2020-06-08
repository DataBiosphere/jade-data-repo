package bio.terra.service.snapshot;

import java.time.Instant;
import java.util.UUID;

public class SnapshotSummary {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;
    private UUID profileId;

    public UUID getId() {
        return id;
    }

    public SnapshotSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public SnapshotSummary name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SnapshotSummary description(String description) {
        this.description = description;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public SnapshotSummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public UUID getProfileId() {
        return profileId;
    }

    public SnapshotSummary profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }
}
