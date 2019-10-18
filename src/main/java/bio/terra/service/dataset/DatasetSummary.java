package bio.terra.service.dataset;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class DatasetSummary {
    private UUID id;
    private String name;
    private String description;
    private UUID defaultProfileId;
    private List<UUID> additionalProfileIds;
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

    public List<UUID> getAdditionalProfileIds() {
        return additionalProfileIds;
    }

    public DatasetSummary additionalProfileIds(List<UUID> additionalProfileIds) {
        this.additionalProfileIds = additionalProfileIds;
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
