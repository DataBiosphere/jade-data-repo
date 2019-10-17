package bio.terra.service.resourcemanagement.google;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GoogleProjectRequest {
    private UUID profileId;
    private String projectId;
    private List<String> serviceIds;
    private Map<String, List<String>> roleIdentityMapping;

    public UUID getProfileId() {
        return profileId;
    }

    public GoogleProjectRequest profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public String getProjectId() {
        return projectId;
    }

    public GoogleProjectRequest projectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public List<String> getServiceIds() {
        return serviceIds;
    }

    public GoogleProjectRequest serviceIds(List<String> serviceIds) {
        this.serviceIds = serviceIds;
        return this;
    }

    // map of iam permissions to list of identities (things the list of id'ed users can do)

    public Map<String, List<String>> getRoleIdentityMapping() {
        return roleIdentityMapping;
    }

    public GoogleProjectRequest roleIdentityMapping(Map<String, List<String>> roleIdentityMapping) {
        this.roleIdentityMapping = roleIdentityMapping;
        return this;
    }
}
