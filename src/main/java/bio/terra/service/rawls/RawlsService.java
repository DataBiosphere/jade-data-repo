package bio.terra.service.rawls;

import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.app.model.rawls.WorkspaceDetails;
import bio.terra.app.model.rawls.WorkspaceResponse;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.WorkspacePolicyModel;
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

  public List<WorkspacePolicyModel> resolvePolicyEmails(
      PolicyModel policyModel, AuthenticatedUserRequest userRequest) {
    Map<UUID, List<ResourcePolicyModel>> workspaceToPolicy =
        policyModel.getMemberPolicies().stream()
            .filter(p -> p.getResourceTypeName().equals("workspace"))
            .collect(Collectors.groupingBy(ResourcePolicyModel::getResourceId));

    return workspaceToPolicy.entrySet().stream()
        .map(
            entry -> {
              UUID workspaceId = entry.getKey();
              WorkspaceResponse workspace = rawlsClient.getWorkspace(workspaceId, userRequest);
              return workspace.toWorkspacePolicyModel(
                  entry.getValue(), terraConfiguration.getBasePath());
            })
        .collect(Collectors.toList());
  }
}
