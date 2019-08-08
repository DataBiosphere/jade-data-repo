package bio.terra.resourcemanagement.metadata.google;

import java.util.UUID;

public class GoogleBucketResource {
    private UUID repositoryId;
    private GoogleBucketRequest googleBucketRequest;
    private GoogleProjectResource googleProjectResource;

    public GoogleBucketResource() {
        this.googleBucketRequest = new GoogleBucketRequest();
        this.googleProjectResource = new GoogleProjectResource();
    }

    public GoogleBucketResource(GoogleBucketRequest googleBucketRequest) {
        this.googleBucketRequest = googleBucketRequest;
        this.googleProjectResource = googleBucketRequest.getGoogleProjectResource();
    }

    public UUID getProjectResourceId() {
        return googleProjectResource.getRepositoryId();
    }

    public GoogleBucketResource projectResourceId(UUID projectResourceId) {
        googleProjectResource.repositoryId(projectResourceId);
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectResource.getGoogleProjectId();
    }

    public GoogleBucketResource googleProjectId(String googleProjectId) {
        googleProjectResource.googleProjectId(googleProjectId);
        return this;
    }

    public UUID getProfileId() {
        return googleBucketRequest.getProfileId();
    }

    public GoogleBucketResource profileId(UUID profileId) {
        googleBucketRequest.profileId(profileId);
        return this;
    }

    public UUID getRepositoryId() {
        return repositoryId;
    }

    public GoogleBucketResource repositoryId(UUID repositoryId) {
        this.repositoryId = repositoryId;
        return this;
    }

    public String getName() {
        return googleBucketRequest.getBucketName();
    }

    public GoogleBucketResource name(String bucketName) {
        googleBucketRequest.bucketName(bucketName);
        return this;
    }
}
