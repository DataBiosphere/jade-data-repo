package bio.terra.service.rawls;

import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.app.model.rawls.WorkspaceDetails;
import bio.terra.app.model.rawls.WorkspaceResponse;
import bio.terra.app.utils.PolicyUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.ErrorModel;
import bio.terra.model.InaccessibleWorkspacePolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.WorkspacePolicyModel;
import bio.terra.service.auth.iam.IamResourceType;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RawlsService {
  private final RawlsClient rawlsClient;
  private final TerraConfiguration terraConfiguration;

  @Autowired
  public RawlsService(TerraConfiguration terraConfiguration, RawlsClient rawlsClient) {
    this.rawlsClient = rawlsClient;
    this.terraConfiguration = terraConfiguration;
  }

  public record WorkspacePolicyModels(
      List<WorkspacePolicyModel> accessible, List<InaccessibleWorkspacePolicyModel> inaccessible) {}

  /**
   * @param samPolicyModel a policy from SAM with member policies
   * @param userRequest authenticated user
   * @return workspaces derived from policy emails, both accessible and inaccessible to the user
   */
  public WorkspacePolicyModels resolvePolicyEmails(
      SamPolicyModel samPolicyModel, AuthenticatedUserRequest userRequest) {
    Map<UUID, List<ResourcePolicyModel>> workspaceToPolicy =
        samPolicyModel.getMemberPolicies().stream()
            .filter(p -> p.getResourceTypeName().equals(IamResourceType.WORKSPACE.toString()))
            .collect(Collectors.groupingBy(ResourcePolicyModel::getResourceId));

    List<WorkspacePolicyModel> accessible = new ArrayList<>();
    List<InaccessibleWorkspacePolicyModel> inaccessible = new ArrayList<>();
    workspaceToPolicy.entrySet().stream()
        .forEach(
            entry -> {
              UUID workspaceId = entry.getKey();
              List<ResourcePolicyModel> policies = entry.getValue();
              try {
                WorkspaceResponse response = rawlsClient.getWorkspace(workspaceId, userRequest);
                WorkspacePolicyModel workspacePolicyModel =
                    response
                        .toWorkspacePolicyModel(policies)
                        .workspaceLink(getWorkspaceLink(response));
                accessible.add(workspacePolicyModel);
              } catch (Exception ex) {
                inaccessible.add(toInaccessibleWorkspacePolicyModel(workspaceId, policies, ex));
              }
            });
    return new WorkspacePolicyModels(accessible, inaccessible);
  }

  public InaccessibleWorkspacePolicyModel toInaccessibleWorkspacePolicyModel(
      UUID workspaceId, List<ResourcePolicyModel> policies, Exception ex) {
    return new InaccessibleWorkspacePolicyModel()
        .workspaceId(workspaceId)
        .workspacePolicies(PolicyUtils.resourcePolicyToPolicyModel(policies))
        .error(new ErrorModel().message(ex.getMessage()));
  }

  /**
   * @return a link to the workspace in Terra UI
   */
  @VisibleForTesting
  String getWorkspaceLink(WorkspaceResponse workspaceResponse) {
    WorkspaceDetails workspace = workspaceResponse.getWorkspace();
    if (workspace == null) {
      return null;
    }
    return "%s/#workspaces/%s/%s"
        .formatted(terraConfiguration.basePath(), workspace.getNamespace(), workspace.getName());
  }
}
