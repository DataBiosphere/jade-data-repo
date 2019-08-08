package bio.terra.metadata;

import java.util.UUID;

public class FileDataProjectSummary {

    private UUID id;
    private UUID fileObjectId;
    private UUID projectResourceId;

    public UUID getId() {
        return id;
    }

    public FileDataProjectSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public UUID getFileObjectId() {
        return fileObjectId;
    }

    public FileDataProjectSummary fileObjectId(UUID fileObjectId) {
        this.fileObjectId = fileObjectId;
        return this;
    }

    public UUID getProjectResourceId() {
        return projectResourceId;
    }

    public FileDataProjectSummary projectResourceId(UUID projectResourceId) {
        this.projectResourceId = projectResourceId;
        return this;
    }
}
