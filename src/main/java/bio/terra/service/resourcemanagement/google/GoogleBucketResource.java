package bio.terra.service.resourcemanagement.google;

import java.util.UUID;

public class GoogleBucketResource {
    private UUID resourceId;
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

    public UUID getResourceId() {
        return resourceId;
    }

    public GoogleBucketResource resourceId(UUID resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public String getName() {
        return googleBucketRequest.getBucketName();
    }

    public GoogleBucketResource name(String bucketName) {
        googleBucketRequest.bucketName(bucketName);
        return this;
    }

    public String getRegion() {
        return googleBucketRequest.getRegion();
    }

    public GoogleBucketResource region(String region) {
        googleBucketRequest.region(region);
        return this;
    }
}
