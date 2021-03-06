package bio.terra.service.dataset;

import java.time.Instant;
import java.util.UUID;

public class DatasetSummary {
    private UUID id;
    private String name;
    private String description;
    private UUID defaultProfileId;
    private UUID projectResourceId;
    private Instant createdDate;

    public UUID getId() {
        return id;
    }

    public DatasetSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DatasetSummary name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DatasetSummary description(String description) {
        this.description = description;
        return this;
    }

    public UUID getDefaultProfileId() {
        return defaultProfileId;
    }

    public DatasetSummary defaultProfileId(UUID defaultProfileId) {
        this.defaultProfileId = defaultProfileId;
        return this;
    }

    public UUID getProjectResourceId() {
        return projectResourceId;
    }

    public DatasetSummary projectResourceId(UUID projectResourceId) {
        this.projectResourceId = projectResourceId;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public DatasetSummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }
}
