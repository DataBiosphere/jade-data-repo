package bio.terra.metadata;

import java.util.UUID;

public class FileDataBucketSummary {
    private UUID id;
    private UUID fileObjectId;
    private UUID bucketResourceId;

    public UUID getId() {
        return id;
    }

    public FileDataBucketSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public UUID getFileObjectId() {
        return fileObjectId;
    }

    public FileDataBucketSummary fileObjectId(UUID fileObjectId) {
        this.fileObjectId = fileObjectId;
        return this;
    }

    public UUID getBucketResourceId() {
        return bucketResourceId;
    }

    public FileDataBucketSummary bucketResourceId(UUID bucketResourceId) {
        this.bucketResourceId = bucketResourceId;
        return this;
    }
}
