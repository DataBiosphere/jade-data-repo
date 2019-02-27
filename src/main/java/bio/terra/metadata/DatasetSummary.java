package bio.terra.metadata;

import java.sql.Timestamp;
import java.util.UUID;

public class DatasetSummary {
    private UUID id;
    private String name;
    private String description;
    private Timestamp createdDate;

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

    public Timestamp getCreatedDate() {
        return new Timestamp(createdDate.getTime());
    }

    public DatasetSummary createdDate(Timestamp createdDate) {
        this.createdDate = new Timestamp(createdDate.getTime());
        return this;
    }
}
