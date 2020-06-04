package bio.terra.service.resourcemanagement.google;

import java.util.UUID;

public class GoogleBucketResource {
    private UUID resourceId;
    private String flightId;
    private UUID profileId;
    private GoogleProjectResource projectResource;
    private String name;
    private String region;

    // Default constructor for Jackson
    public GoogleBucketResource() { }

    // Construct from a request
    public GoogleBucketResource(GoogleBucketRequest googleBucketRequest) {
        this.profileId = googleBucketRequest.getProfileId();
        this.projectResource = googleBucketRequest.getGoogleProjectResource();
        this.name = googleBucketRequest.getBucketName();
        this.region = googleBucketRequest.getRegion();
    }

    public UUID getResourceId() {
        return resourceId;
    }

    public GoogleBucketResource resourceId(UUID resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public String getFlightId() {
        return flightId;
    }

    public GoogleBucketResource flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }

    public UUID getProfileId() {
        return profileId;
    }

    public GoogleBucketResource profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public GoogleProjectResource getProjectResource() {
        return projectResource;
    }

    public GoogleBucketResource projectResource(GoogleProjectResource projectResource) {
        this.projectResource = projectResource;
        return this;
    }

    public String getName() {
        return name;
    }

    public GoogleBucketResource name(String name) {
        this.name = name;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public GoogleBucketResource region(String region) {
        this.region = region;
        return this;
    }
}
