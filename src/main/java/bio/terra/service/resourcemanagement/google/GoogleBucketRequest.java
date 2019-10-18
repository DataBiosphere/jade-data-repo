package bio.terra.service.resourcemanagement.google;

import java.util.UUID;

public class GoogleBucketRequest {
    private UUID profileId;
    private GoogleProjectResource googleProjectResource;
    private String bucketName;
    private String region;

    public UUID getProfileId() {
        return profileId;
    }

    public GoogleBucketRequest profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public GoogleProjectResource getGoogleProjectResource() {
        return googleProjectResource;
    }

    public GoogleBucketRequest googleProjectResource(GoogleProjectResource googleProjectResource) {
        this.googleProjectResource = googleProjectResource;
        return this;
    }

    public String getBucketName() {
        return bucketName;
    }

    public GoogleBucketRequest bucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public GoogleBucketRequest region(String region) {
        this.region = region;
        return this;
    }
}
