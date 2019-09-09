package bio.terra.resourcemanagement.metadata.google;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GoogleProjectRequest {
    private UUID profileId;
    private String projectId;
    private List<String> serviceIds;
    private Map<String, List<String>> userPermissions;

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

    // map of identities to list of iam permissions (a list of things the id'ed user can do)

    public Map<String, List<String>> getUserPermissions() {
        return userPermissions;
    }

    public GoogleProjectRequest userPermissions(Map<String, List<String>> userPermissions) {
        this.userPermissions = userPermissions;
        return this;
    }
}
