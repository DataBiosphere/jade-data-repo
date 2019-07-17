package bio.terra.resourcemanagement.metadata.google;

import java.util.List;
import java.util.UUID;

public class GoogleProjectResource {

    private GoogleProjectRequest googleProjectRequest;
    private UUID repositoryId;
    private String googleProjectNumber;

    public GoogleProjectResource() {
        this.googleProjectRequest = new GoogleProjectRequest();
    }

    public GoogleProjectResource(GoogleProjectRequest googleProjectRequest) {
        this.googleProjectRequest = googleProjectRequest;
    }

    public String getGoogleProjectId() {
        return googleProjectRequest.getProjectId();
    }

    public GoogleProjectResource googleProjectId(String googleProjectId) {
        googleProjectRequest.projectId(googleProjectId);
        return this;
    }

    public UUID getProfileId() {
        return googleProjectRequest.getProfileId();
    }

    public GoogleProjectResource profileId(UUID profileId) {
        googleProjectRequest.profileId(profileId);
        return this;
    }

    public List<String> getServiceIds() {
        return googleProjectRequest.getServiceIds();
    }

    public GoogleProjectResource serviceIds(List<String> serviceIds) {
        googleProjectRequest.serviceIds(serviceIds);
        return this;
    }

    public UUID getRepositoryId() {
        return repositoryId;
    }

    public GoogleProjectResource repositoryId(UUID repositoryId) {
        this.repositoryId = repositoryId;
        return this;
    }

    public String getGoogleProjectNumber() {
        return googleProjectNumber;
    }

    public GoogleProjectResource googleProjectNumber(String googleProjectNumber) {
        this.googleProjectNumber = googleProjectNumber;
        return this;
    }
}
