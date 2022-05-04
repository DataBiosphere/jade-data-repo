package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.rawls.WorkspaceDetails;
import bio.terra.app.model.rawls.WorkspaceResponse;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
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

  private UUID workspaceId;
  private String workspaceName;
  private String workspaceNamepace;

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

    workspaceId = UUID.randomUUID();
    workspaceName = "testWorkspace";
    workspaceNamepace = "testNamespace";

    when(rawlsClient.getWorkspace(any(), any()))
        .thenReturn(
            new WorkspaceResponse()
                .workspace(
                    new WorkspaceDetails()
                        .workspaceId(workspaceId.toString())
                        .name(workspaceName)
                        .namespace(workspaceNamepace)));
  }

  @Test
  public void testRetrievePoliciesAndEmailsWorkspace() throws Exception {
    final UUID id = UUID.randomUUID();
    final String policyEmail = "policygroup@firecloud.org";
    final String memberEmail = "a@a.com";

    final String ownerName = "owner";
    final String ownerEmail = String.format("policy-%s@firecloud.org", UUID.randomUUID());
    final String resourceTypeName = "workspace";
    final UUID resourceId = UUID.randomUUID();
    when(samResourceApi.listResourcePoliciesV2(
            IamResourceType.DATASNAPSHOT.getSamResourceName(), id.toString()))
        .thenReturn(
            List.of(
                new AccessPolicyResponseEntryV2()
                    .policyName(IamRole.READER.toString())
                    .email(policyEmail)
                    .policy(
                        new AccessPolicyMembershipV2()
                            .addMemberEmailsItem(memberEmail)
                            .addMemberPoliciesItem(
                                new PolicyIdentifiers()
                                    .policyName(ownerName)
                                    .policyEmail(ownerEmail)
                                    .resourceTypeName(resourceTypeName)
                                    .resourceId(resourceId.toString())))));

    List<SamPolicyModel> samPolicyModels =
        samIam.retrievePolicies(userReq, IamResourceType.DATASNAPSHOT, id);
    assertThat(
        samPolicyModels,
        is(
            List.of(
                new SamPolicyModel()
                    .name(IamRole.READER.toString())
                    .addMembersItem(memberEmail)
                    .addMemberPoliciesItem(
                        new ResourcePolicyModel()
                            .policyEmail(ownerEmail)
                            .policyName(ownerName)
                            .resourceTypeName(resourceTypeName)
                            .resourceId(resourceId)))));

    List<WorkspacePolicyModel> workspacePolicyModels =
        rawlsService.resolvePolicyEmails(samPolicyModels.get(0), userReq);

    assertThat(
        workspacePolicyModels,
        is(
            List.of(
                new WorkspacePolicyModel()
                    .workspaceName(workspaceName)
                    .workspaceNamespace(workspaceNamepace)
                    .workspaceId(workspaceId)
                    .workspaceLink(
                        "https://bvdp-saturn-dev.appspot.com/#workspaces/testNamespace/testWorkspace")
                    .addWorkspacePoliciesItem(
                        new PolicyModel().name(ownerName).addMembersItem(ownerEmail)))));
  }
}
