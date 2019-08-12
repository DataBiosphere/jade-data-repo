package bio.terra.metadata;

import bio.terra.resourcemanagement.metadata.google.GoogleBucketResource;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;

import java.util.Optional;
import java.util.UUID;

public class FileDataBucket {

    private FileDataBucketSummary fileDataBucketSummary = new FileDataBucketSummary();
    private GoogleBucketResource googleBucketResource = new GoogleBucketResource();

    public FileDataBucket() {}

    public FileDataBucket(FileDataBucketSummary fileDataBucketSummary) {
        this.fileDataBucketSummary = fileDataBucketSummary;
    }

    public FileDataBucketSummary getFileDataBucketSummary() {
        return fileDataBucketSummary;
    }

    public FileDataBucket setFileDataBucketSummary(FileDataBucketSummary fileDataBucketSummary) {
        this.fileDataBucketSummary = fileDataBucketSummary;
        return this;
    }

    public GoogleBucketResource getGoogleBucketResource() {
        return googleBucketResource;
    }

    public FileDataBucket googleBucketResource(GoogleBucketResource projectResource) {
        this.googleBucketResource = projectResource;
        return this;
    }

    public String getName() {
        return googleBucketResource.getName();
    }

    public FileDataBucket name(String name) {
        googleBucketResource.name(name);
        return this;
    }

    public Optional<GoogleProjectResource> getProjectResource() {
        return Optional.ofNullable(googleBucketResource.getProjectResource());
    }

    public UUID getProfileId() {
        return googleBucketResource.getProfileId();
    }

    public FileDataBucket profileId(UUID profileId) {
        googleBucketResource.profileId(profileId);
        return this;
    }

    public UUID getProjectResourceId() {
        return googleBucketResource.getRepositoryId();
    }

    public FileDataBucket projectResourceId(UUID projectResourceId) {
        googleBucketResource.repositoryId(projectResourceId);
        return this;
    }

    public UUID getId() {
        return fileDataBucketSummary.getId();
    }

    public FileDataBucket id(UUID id) {
        fileDataBucketSummary.id(id);
        return this;
    }

    public UUID getFileObjectId() {
        return fileDataBucketSummary.getFileObjectId();
    }

    public FileDataBucket fileObjectId(UUID fileObjectId) {
        fileDataBucketSummary.fileObjectId(fileObjectId);
        return this;
    }
}
