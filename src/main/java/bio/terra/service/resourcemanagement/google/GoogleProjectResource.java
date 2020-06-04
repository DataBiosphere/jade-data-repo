package bio.terra.service.resourcemanagement.google;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GoogleProjectResource {
    private UUID profileId;
    private String googleProjectId;
    private List<String> serviceIds;
    private Map<String, List<String>> roleIdentityMapping;
    private UUID repositoryId;
    private String googleProjectNumber;

    // Default constructor for JSON serdes
    public GoogleProjectResource() {
    }

    public GoogleProjectResource(GoogleProjectRequest googleProjectRequest) {
        this.profileId = googleProjectRequest.getProfileId();
        this.googleProjectId = googleProjectRequest.getProjectId();
        this.serviceIds = googleProjectRequest.getServiceIds();
        this.roleIdentityMapping = googleProjectRequest.getRoleIdentityMapping();
    }

    public UUID getProfileId() {
        return profileId;
    }

    public GoogleProjectResource profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectId;
    }

    public GoogleProjectResource googleProjectId(String googleProjectId) {
        this.googleProjectId = googleProjectId;
        return this;
    }

    public List<String> getServiceIds() {
        return serviceIds;
    }

    public GoogleProjectResource serviceIds(List<String> serviceIds) {
        this.serviceIds = serviceIds;
        return this;
    }

    public Map<String, List<String>> getRoleIdentityMapping() {
        return roleIdentityMapping;
    }

    public GoogleProjectResource roleIdentityMapping(Map<String, List<String>> roleIdentityMapping) {
        this.roleIdentityMapping = roleIdentityMapping;
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
