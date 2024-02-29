package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.ErrorModel;
import bio.terra.model.InaccessibleWorkspacePolicyModel;
import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.WorkspacePolicyModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class RawlsServiceTest {

  @Mock private TerraConfiguration terraConfiguration;
  @Mock private RawlsClient rawlsClient;
  private RawlsService rawlsService;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String BASE_PATH = "terra.base.path";
  private static final String OWNER_NAME = "owner";
  private static final String OWNER_EMAIL = "policy-email@firecloud.org";
  private static final String WORKSPACE_NAME = "testWorkspace";
  private static final String WORKSPACE_NAMESPACE = "testNamespace";
  private static final String WORKSPACE_LINK =
      "%s/#workspaces/%s/%s".formatted(BASE_PATH, WORKSPACE_NAMESPACE, WORKSPACE_NAME);

  private final UUID accessibleWorkspaceId = UUID.randomUUID();
  private final WorkspaceResponse accessibleWorkspaceResponse =
      new WorkspaceResponse(
          new WorkspaceDetails(
              accessibleWorkspaceId.toString(), WORKSPACE_NAMESPACE, WORKSPACE_NAME));
  private final UUID inaccessibleWorkspaceId = UUID.randomUUID();
  private final UnauthorizedException inaccessibleWorkspaceException =
      new UnauthorizedException("Workspace inaccessible");

  @BeforeEach
  void beforeEach() {
    rawlsService = new RawlsService(terraConfiguration, rawlsClient);

    when(terraConfiguration.basePath()).thenReturn(BASE_PATH);

    when(rawlsClient.getWorkspace(accessibleWorkspaceId, TEST_USER))
        .thenReturn(accessibleWorkspaceResponse);
    when(rawlsClient.getWorkspace(inaccessibleWorkspaceId, TEST_USER))
        .thenThrow(inaccessibleWorkspaceException);
  }

  private ResourcePolicyModel createResourcePolicyModel(
      IamResourceType resourceType, UUID resourceId) {
    return new ResourcePolicyModel()
        .policyName(OWNER_NAME)
        .policyEmail(OWNER_EMAIL)
        .resourceTypeName(resourceType.toString())
        .resourceId(resourceId);
  }

  @Test
  void testRetrievePoliciesAndEmailsWorkspace() {
    final UUID nonWorkspaceResourceId = UUID.randomUUID();
    final String memberEmail = "a@a.com";

    var resourcePolicyModels =
        List.of(
            createResourcePolicyModel(IamResourceType.WORKSPACE, accessibleWorkspaceId),
            createResourcePolicyModel(IamResourceType.WORKSPACE, inaccessibleWorkspaceId),
            createResourcePolicyModel(IamResourceType.DATASNAPSHOT, nonWorkspaceResourceId));
    SamPolicyModel samPolicyModel =
        new SamPolicyModel()
            .name(IamRole.READER.toString())
            .addMembersItem(memberEmail)
            .memberPolicies(resourcePolicyModels);

    var workspacePolicyModels = rawlsService.resolvePolicyEmails(samPolicyModel, TEST_USER);

    assertThat(
        "Workspace accessible to the user is included in returned policy models",
        workspacePolicyModels.accessible(),
        contains(
            new WorkspacePolicyModel()
                .workspaceName(WORKSPACE_NAME)
                .workspaceNamespace(WORKSPACE_NAMESPACE)
                .workspaceId(accessibleWorkspaceId)
                .workspaceLink(WORKSPACE_LINK)
                .addWorkspacePoliciesItem(
                    new PolicyModel().name(OWNER_NAME).addMembersItem(OWNER_EMAIL))));
    assertThat(
        "Workspace inaccessible to the user is included in returned policy models",
        workspacePolicyModels.inaccessible(),
        contains(
            new InaccessibleWorkspacePolicyModel()
                .workspaceId(inaccessibleWorkspaceId)
                .addWorkspacePoliciesItem(
                    new PolicyModel().name(OWNER_NAME).addMembersItem(OWNER_EMAIL))
                .error(new ErrorModel().message(inaccessibleWorkspaceException.getMessage()))));
  }
}
