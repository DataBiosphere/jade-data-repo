package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.rawls.WorkspaceDetails;
import bio.terra.app.model.rawls.WorkspaceResponse;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.ErrorModel;
import bio.terra.model.InaccessibleWorkspacePolicyModel;
import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.WorkspacePolicyModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.sam.SamIam;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.List;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipV2;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntryV2;
import org.broadinstitute.dsde.workbench.client.sam.model.PolicyIdentifiers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class RawlsServiceTest {

  @Mock private SamConfiguration samConfig;
  @Mock private ConfigurationService configurationService;
  @Mock private ApiClient apiClient;
  @Mock private ResourcesApi samResourceApi;
  @Mock private StatusApi samStatusApi;
  @Mock private GoogleApi samGoogleApi;
  @Mock private UsersApi samUsersApi;

  @Mock private AuthenticatedUserRequest userReq;

  @MockBean private RawlsClient rawlsClient;
  @Autowired private RawlsService rawlsService;
  private SamIam samIam;
  private static final String OWNER_NAME = "owner";
  private static final String OWNER_EMAIL = "policy-email@firecloud.org";
  private static final String WORKSPACE_NAME = "testWorkspace";
  private static final String WORKSPACE_NAMEPACE = "testNamespace";
  private final UUID accessibleWorkspaceId = UUID.randomUUID();
  private final UUID inaccessibleWorkspaceId = UUID.randomUUID();
  private final UnauthorizedException inaccessibleWorkspaceException =
      new UnauthorizedException("Workspace inaccessible");

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    samIam = spy(new SamIam(samConfig, configurationService));
    final String userToken = "some_token";
    when(userReq.getToken()).thenReturn(userToken);
    when(configurationService.getParameterValue(ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS))
        .thenReturn(0);
    when(configurationService.getParameterValue(ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS))
        .thenReturn(0);
    when(configurationService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS))
        .thenReturn(0);
    // Mock out samApi, samStatusApi, samGoogleApi, and samUsersApi in individual tests as needed
    doAnswer(a -> samResourceApi).when(samIam).samResourcesApi(userToken);
    // Mock out the lower level client in individual as needed
    when(samResourceApi.getApiClient()).thenAnswer(a -> apiClient);

    when(rawlsClient.getWorkspace(accessibleWorkspaceId, userReq))
        .thenReturn(
            new WorkspaceResponse()
                .workspace(
                    new WorkspaceDetails()
                        .workspaceId(accessibleWorkspaceId.toString())
                        .name(WORKSPACE_NAME)
                        .namespace(WORKSPACE_NAMEPACE)));

    when(rawlsClient.getWorkspace(inaccessibleWorkspaceId, userReq))
        .thenThrow(inaccessibleWorkspaceException);
  }

  private PolicyIdentifiers createPolicyIdentifiers(IamResourceType resourceType, UUID resourceId) {
    return new PolicyIdentifiers()
        .policyName(OWNER_NAME)
        .policyEmail(OWNER_EMAIL)
        .resourceTypeName(resourceType.toString())
        .resourceId(resourceId.toString());
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
  public void testRetrievePoliciesAndEmailsWorkspace() throws Exception {
    final UUID snapshotId = UUID.randomUUID();
    final UUID nonWorkspaceResourceId = UUID.randomUUID();
    final String policyEmail = "policygroup@firecloud.org";
    final String memberEmail = "a@a.com";

    var policyIdentifiers =
        List.of(
            createPolicyIdentifiers(IamResourceType.WORKSPACE, accessibleWorkspaceId),
            createPolicyIdentifiers(IamResourceType.WORKSPACE, inaccessibleWorkspaceId),
            createPolicyIdentifiers(IamResourceType.DATASNAPSHOT, nonWorkspaceResourceId));
    var workspacePolicy =
        new AccessPolicyResponseEntryV2()
            .policyName(IamRole.READER.toString())
            .email(policyEmail)
            .policy(
                new AccessPolicyMembershipV2()
                    .addMemberEmailsItem(memberEmail)
                    .memberPolicies(policyIdentifiers));
    when(samResourceApi.listResourcePoliciesV2(
            IamResourceType.DATASNAPSHOT.toString(), snapshotId.toString()))
        .thenReturn(List.of(workspacePolicy));

    var resourcePolicyModels =
        List.of(
            createResourcePolicyModel(IamResourceType.WORKSPACE, accessibleWorkspaceId),
            createResourcePolicyModel(IamResourceType.WORKSPACE, inaccessibleWorkspaceId),
            createResourcePolicyModel(IamResourceType.DATASNAPSHOT, nonWorkspaceResourceId));
    List<SamPolicyModel> samPolicyModels =
        samIam.retrievePolicies(userReq, IamResourceType.DATASNAPSHOT, snapshotId);
    assertThat(
        "Snapshot resource policies for all resource types are returned by SAM",
        samPolicyModels,
        is(
            List.of(
                new SamPolicyModel()
                    .name(IamRole.READER.toString())
                    .addMembersItem(memberEmail)
                    .memberPolicies(resourcePolicyModels))));

    var workspacePolicyModels = rawlsService.resolvePolicyEmails(samPolicyModels.get(0), userReq);
    verify(rawlsClient, times(1)).getWorkspace(accessibleWorkspaceId, userReq);
    verify(rawlsClient, times(1)).getWorkspace(inaccessibleWorkspaceId, userReq);

    assertThat(
        "Workspace accessible to the user is included in returned policy models",
        workspacePolicyModels.accessible(),
        is(
            List.of(
                new WorkspacePolicyModel()
                    .workspaceName(WORKSPACE_NAME)
                    .workspaceNamespace(WORKSPACE_NAMEPACE)
                    .workspaceId(accessibleWorkspaceId)
                    .workspaceLink(
                        "https://bvdp-saturn-dev.appspot.com/#workspaces/testNamespace/testWorkspace")
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
