package bio.terra.metadata;

import java.time.Instant;
import java.util.UUID;

public class DrDatasetSummary {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;

    public DrDatasetSummary() {
    }

    public DrDatasetSummary(DrDatasetSummary summary) {
        this.id(summary.getId())
            .name(summary.getName())
            .description(summary.getDescription())
            .createdDate(summary.getCreatedDate());
    }

    public UUID getId() {
        return id;
    }

    public DrDatasetSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DrDatasetSummary name(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DrDatasetSummary description(String description) {
        this.description = description;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public DrDatasetSummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

}
