package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.app.model.rawls.WorkspaceDetails;
import bio.terra.app.model.rawls.WorkspaceResponse;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RawlsServiceTest {

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
  private final UUID accessibleWorkspaceId = UUID.randomUUID();
  private final WorkspaceResponse accessibleWorkspaceResponse =
      new WorkspaceResponse()
          .workspace(
              new WorkspaceDetails()
                  .workspaceId(accessibleWorkspaceId.toString())
                  .name(WORKSPACE_NAME)
                  .namespace(WORKSPACE_NAMESPACE));
  private final UUID inaccessibleWorkspaceId = UUID.randomUUID();
  private final UnauthorizedException inaccessibleWorkspaceException =
      new UnauthorizedException("Workspace inaccessible");

  @Before
  public void setUp() throws Exception {
    rawlsService = new RawlsService(terraConfiguration, rawlsClient);

    when(terraConfiguration.getBasePath()).thenReturn(BASE_PATH);

    when(rawlsClient.getWorkspace(accessibleWorkspaceId, TEST_USER))
        .thenReturn(accessibleWorkspaceResponse);
    when(rawlsClient.getWorkspace(inaccessibleWorkspaceId, TEST_USER))
        .thenThrow(inaccessibleWorkspaceException);
  }

  @Test
  public void testGetWorkspaceLink() {
    assertThat(
        "A workspace response with no workspace yields a null workspace link",
        rawlsService.getWorkspaceLink(new WorkspaceResponse()),
        nullValue());

    assertThat(accessibleWorkspaceResponse.getWorkspace(), not(nullValue()));
    assertThat(
        "Link can be constructed for a non-null workspace",
        rawlsService.getWorkspaceLink(accessibleWorkspaceResponse),
        startsWith(BASE_PATH));
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
  public void testRetrievePoliciesAndEmailsWorkspace() {
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
    verify(rawlsClient).getWorkspace(accessibleWorkspaceId, TEST_USER);
    verify(rawlsClient).getWorkspace(inaccessibleWorkspaceId, TEST_USER);

    assertThat(
        "Workspace accessible to the user is included in returned policy models",
        workspacePolicyModels.accessible(),
        is(
            List.of(
                new WorkspacePolicyModel()
                    .workspaceName(WORKSPACE_NAME)
                    .workspaceNamespace(WORKSPACE_NAMESPACE)
                    .workspaceId(accessibleWorkspaceId)
                    .workspaceLink(rawlsService.getWorkspaceLink(accessibleWorkspaceResponse))
                    .addWorkspacePoliciesItem(
                        new PolicyModel().name(OWNER_NAME).addMembersItem(OWNER_EMAIL)))));

    assertThat(
        "Workspace inaccessible to the user is included in returned policy models",
        workspacePolicyModels.inaccessible(),
        is(
            List.of(
                new InaccessibleWorkspacePolicyModel()
                    .workspaceId(inaccessibleWorkspaceId)
                    .addWorkspacePoliciesItem(
                        new PolicyModel().name(OWNER_NAME).addMembersItem(OWNER_EMAIL))
                    .error(
                        new ErrorModel().message(inaccessibleWorkspaceException.getMessage())))));
  }
}
