package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.WorkspacePolicyModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class WorkspaceResponseTest {
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";
  private static final String POLICY_EMAIL = "policy@a.com";
  private static final String POLICY_NAME = "policyName";

  @Test
  void workspaceResponse() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    WorkspaceDetails workspaceDetails =
        new WorkspaceDetails(WORKSPACE_ID.toString(), NAMESPACE, NAME);
    String content =
        String.format(
            "{\"workspace\": %s, \"unknown\": \"property\"}",
            objectMapper.writeValueAsString(workspaceDetails));
    assertThat(
        objectMapper.readValue(content, WorkspaceResponse.class),
        equalTo(new WorkspaceResponse(workspaceDetails)));
  }

  @Test
  void toWorkspacePolicyModel() {
    WorkspaceResponse workspaceResponse =
        new WorkspaceResponse(new WorkspaceDetails(WORKSPACE_ID.toString(), NAMESPACE, NAME));
    ResourcePolicyModel resourcePolicyModel =
        new ResourcePolicyModel()
            .resourceId(UUID.randomUUID())
            .policyEmail(POLICY_EMAIL)
            .policyName(POLICY_NAME);

    assertThat(
        workspaceResponse.toWorkspacePolicyModel(List.of(resourcePolicyModel)),
        equalTo(
            new WorkspacePolicyModel()
                .workspaceId(WORKSPACE_ID)
                .workspaceNamespace(NAMESPACE)
                .workspaceName(NAME)
                .addWorkspacePoliciesItem(
                    new PolicyModel().name(POLICY_NAME).addMembersItem(POLICY_EMAIL))));
  }
}
