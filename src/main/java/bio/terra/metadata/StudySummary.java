package bio.terra.metadata;

import java.time.Instant;
import java.util.UUID;

public class StudySummary {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;

    public StudySummary() {
    }

    public StudySummary(StudySummary summary) {
        this.id(summary.getId())
                .name(summary.getName())
                .description(summary.getDescription())
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

    public Instant getCreatedDate() {
        return createdDate;
    }

    public StudySummary createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

}
