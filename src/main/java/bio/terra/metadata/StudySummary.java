package bio.terra.metadata;

import java.time.Instant;
import java.util.UUID;

public class StudySummary {
    private UUID id;
    private String name;
    private String description;
    private Instant createdDate;

    public StudySummary() {}
    public StudySummary(StudySummary summary) {
        this.setId(summary.getId())
                .setName(summary.getName())
                .setDescription(summary.getDescription())
                .setCreatedDate(summary.getCreatedDate());
    }

    public UUID getId() { return id; }
    public StudySummary setId(UUID id) { this.id = id; return this; }

    public String getName() {
        return name;
    }
    public StudySummary setName(String name) { this.name = name; return this; }

    public String getDescription() {
        return description;
    }
    public StudySummary setDescription(String description) { this.description = description; return this; }

    public Instant getCreatedDate() {
        return createdDate;
    }
    public StudySummary setCreatedDate(Instant createdDate) { this.createdDate = createdDate; return this; }

}
