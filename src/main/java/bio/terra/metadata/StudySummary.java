package bio.terra.metadata;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class StudySummary {
    private UUID id;
    private String name;
    private String description;
    private UUID defaultProfileId;
    private List<UUID> additionalProfileIds;
    private Instant createdDate;

    public StudySummary() {
    }

    public StudySummary(StudySummary summary) {
        this.id(summary.getId())
            .name(summary.getName())
            .description(summary.getDescription())
            .defaultProfileId(summary.getDefaultProfileId())
            .additionalProfileIds(summary.getAdditionalProfileIds())
            .createdDate(summary.getCreatedDate());
    }

    public UUID getId() {
        return id;
    }

    public StudySummary id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public StudySummary name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudySummary description(String description) {
        this.description = description;
        return this;
    }

    public UUID getDefaultProfileId() {
        return defaultProfileId;
    }

    public StudySummary defaultProfileId(UUID defaultProfileId) {
        this.defaultProfileId = defaultProfileId;
        return this;
    }

    public List<UUID> getAdditionalProfileIds() {
        return additionalProfileIds;
    }

    public StudySummary additionalProfileIds(List<UUID> additionalProfileIds) {
        this.additionalProfileIds = additionalProfileIds;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public StudySummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

}
