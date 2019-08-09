package bio.terra.resourcemanagement.metadata.google;

import java.util.UUID;

public class GoogleBucketResource {
    private UUID repositoryId;
    private GoogleBucketRequest googleBucketRequest;

    public GoogleBucketResource() {
        this.googleBucketRequest = new GoogleBucketRequest();
    }

    public GoogleBucketResource(GoogleBucketRequest googleBucketRequest) {
        this.googleBucketRequest = googleBucketRequest;
    }

    public GoogleProjectResource getProjectResource() {
        return googleBucketRequest.getGoogleProjectResource();
    }

    public GoogleBucketResource projectResource(GoogleProjectResource projectResource) {
        googleBucketRequest.googleProjectResource(projectResource);
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
