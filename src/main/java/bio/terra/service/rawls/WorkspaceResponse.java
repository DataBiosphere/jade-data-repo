package bio.terra.service.rawls;

import bio.terra.app.utils.PolicyUtils;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.WorkspacePolicyModel;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkspaceResponse(WorkspaceDetails workspace) {
  public WorkspacePolicyModel toWorkspacePolicyModel(
      List<ResourcePolicyModel> resourcePolicyModels) {
    return new WorkspacePolicyModel()
        .workspaceId(UUID.fromString(workspace.workspaceId()))
        .workspaceNamespace(workspace.namespace())
        .workspaceName(workspace.name())
        .workspacePolicies(PolicyUtils.resourcePolicyToPolicyModel(resourcePolicyModels));
  }
}
