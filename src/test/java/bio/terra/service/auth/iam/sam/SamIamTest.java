package bio.terra.service.auth.iam.sam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipV2;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntryV2;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.broadinstitute.dsde.workbench.client.sam.model.RolesAndActions;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.model.UserResourcesResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SamIamTest {

  @Mock private SamConfiguration samConfig;
  @Mock private ConfigurationService configurationService;
  @Mock private ApiClient apiClient;
  @Mock private ResourcesApi samResourceApi;
  @Mock private StatusApi samStatusApi;
  @Mock private GoogleApi samGoogleApi;
  @Mock private UsersApi samUsersApi;

  @Mock private AuthenticatedUserRequest userReq;

  private SamIam samIam;

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
    doAnswer(a -> samStatusApi).when(samIam).samStatusApi();
    doAnswer(a -> samGoogleApi).when(samIam).samGoogleApi(userToken);
    doAnswer(a -> samUsersApi).when(samIam).samUsersApi(userToken);
    // Mock out the lower level client in individual as needed
    when(samResourceApi.getApiClient()).thenAnswer(a -> apiClient);
  }

  @Test
  public void testExtractErrorMessageSimple() {
    ErrorReport errorReport = new ErrorReport().message("FOO").source("sam");

    assertThat(SamIam.extractErrorMessage(errorReport), is("FOO"));
  }

  @Test
  public void testExtractErrorMessageSimpleNested() {
    ErrorReport errorReport =
        new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport().message("BAR").source("sam"));

    assertThat(SamIam.extractErrorMessage(errorReport), is("FOO: BAR"));
  }

  @Test
  public void testExtractErrorMessageDeepNested() {
    ErrorReport errorReport =
        new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(
                new ErrorReport()
                    .message("BAR")
                    .source("sam")
                    .addCausesItem(new ErrorReport().message("BAZ1").source("sam"))
                    .addCausesItem(
                        new ErrorReport()
                            .message("BAZ2")
                            .source("sam")
                            .addCausesItem(new ErrorReport().message("QUX").source("sam"))));

    assertThat(SamIam.extractErrorMessage(errorReport), is("FOO: BAR: (BAZ1, BAZ2: QUX)"));
  }

  @Test
  public void testIgnoresNonUUIDResourceName() throws ApiException, InterruptedException {
    final UUID goodId = UUID.randomUUID();
    final String badId = "badUUID";
    when(samResourceApi.listResourcesAndPoliciesV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName()))
        .thenReturn(
            List.of(
                new UserResourcesResponse().resourceId(goodId.toString()),
                new UserResourcesResponse().resourceId(badId)));

    Set<UUID> uuids =
        samIam.listAuthorizedResources(userReq, IamResourceType.SPEND_PROFILE).keySet();
    assertThat(uuids, contains(goodId));
  }

  @Test
  public void testAuthorization() throws ApiException, InterruptedException {
    when(samResourceApi.resourcePermissionV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            "my-id",
            IamAction.READ_DATA.toString()))
        .thenReturn(true);
    when(samResourceApi.resourcePermissionV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            "my-id",
            IamAction.ALTER_POLICIES.toString()))
        .thenReturn(false);
    assertTrue(
        samIam.isAuthorized(userReq, IamResourceType.SPEND_PROFILE, "my-id", IamAction.READ_DATA));
    assertFalse(
        samIam.isAuthorized(
            userReq, IamResourceType.SPEND_PROFILE, "my-id", IamAction.ALTER_POLICIES));
  }

  @Test
  public void testGetStatus() throws ApiException {
    when(samStatusApi.getSystemStatus())
        .thenReturn(
            new SystemStatus().ok(true).systems(Map.of("GooglePubSub", Map.of("ok", true))));
    assertThat(
        samIam.samStatus(),
        is(new RepositoryStatusModelSystems().ok(true).message("{GooglePubSub={ok=true}}")));
  }

  @Test
  public void testGetStatusException() throws ApiException {
    when(samStatusApi.getSystemStatus()).thenThrow(new ApiException("BOOM!"));
    assertThat(
        samIam.samStatus(),
        is(
            new RepositoryStatusModelSystems()
                .ok(false)
                .message(
                    "Sam status check failed: bio.terra.service.auth.iam.exception.IamInternalServerErrorException: "
                        + "BOOM!")));
  }

  @Test
  public void testGetUserInfo() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    assertThat(
        samIam.getUserInfo(userReq),
        is(new UserStatusInfo().userSubjectId(userSubjectId).userEmail(userEmail).enabled(true)));
  }

  @Test
  public void testHasAnyActions() throws ApiException, InterruptedException {
    when(samResourceApi.resourceActionsV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(), "my-id-1"))
        .thenReturn(List.of(IamAction.READ_DATA.toString()));
    when(samResourceApi.resourceActionsV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(), "my-id-2"))
        .thenReturn(List.of());
    assertTrue(samIam.hasAnyActions(userReq, IamResourceType.SPEND_PROFILE, "my-id-1"));
    assertFalse(samIam.hasAnyActions(userReq, IamResourceType.SPEND_PROFILE, "my-id-2"));
  }

  @Test
  public void testDeleteResource() throws InterruptedException, ApiException {
    final UUID datasetId = UUID.randomUUID();
    samIam.deleteDatasetResource(userReq, datasetId);
    // Verify that the correct Sam API call was made
    verify(samResourceApi, times(1))
        .deleteResourceV2(
            eq(IamResourceType.DATASET.getSamResourceName()), eq(datasetId.toString()));

    final UUID snapshotId = UUID.randomUUID();
    samIam.deleteSnapshotResource(userReq, snapshotId);
    verify(samResourceApi, times(1))
        .deleteResourceV2(
            eq(IamResourceType.DATASNAPSHOT.getSamResourceName()), eq(snapshotId.toString()));

    final UUID profileId = UUID.randomUUID();
    samIam.deleteProfileResource(userReq, profileId.toString());
    verify(samResourceApi, times(1))
        .deleteResourceV2(
            eq(IamResourceType.SPEND_PROFILE.getSamResourceName()), eq(profileId.toString()));
  }

  @Test
  public void testRetrievePoliciesAndEmails() throws ApiException, InterruptedException {
    final UUID id = UUID.randomUUID();
    final String policyEmail = "policygroup@firecloud.org";
    final String memberEmail = "a@a.com";
    when(samResourceApi.listResourcePoliciesV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(), id.toString()))
        .thenReturn(
            List.of(
                new AccessPolicyResponseEntryV2()
                    .policyName(IamRole.CUSTODIAN.toString())
                    .email(policyEmail)
                    .policy(new AccessPolicyMembershipV2().addMemberEmailsItem(memberEmail))));

    assertThat(
        samIam.retrievePolicies(userReq, IamResourceType.SPEND_PROFILE, id),
        is(
            List.of(
                new SamPolicyModel()
                    .name(IamRole.CUSTODIAN.toString())
                    .addMembersItem(memberEmail)
                    .memberPolicies(List.of()))));

    assertThat(
        samIam.retrievePolicyEmails(userReq, IamResourceType.SPEND_PROFILE, id),
        is(Map.of(IamRole.CUSTODIAN, policyEmail)));
  }

  @Test
  public void testCreateDataset() throws InterruptedException, ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID datasetId = UUID.randomUUID();
    // Note: in our case, policies have a 1:1 relationship with roles
    final List<IamRole> allPolicies =
        List.of(IamRole.ADMIN, IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR);
    final List<IamRole> syncedPolicies =
        List.of(IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR);

    for (IamRole policy : allPolicies) {
      when(samGoogleApi.syncPolicy(
              IamResourceType.DATASET.getSamResourceName(),
              datasetId.toString(),
              policy.toString()))
          .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", List.of()));
    }

    assertThat(
        samIam.createDatasetResource(userReq, datasetId),
        is(
            syncedPolicies.stream()
                .collect(Collectors.toMap(p -> p, p -> "policygroup-" + p + "@firecloud.org"))));
  }

  @Test
  public void testCreateSnapshot() throws InterruptedException, ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID snapshotId = UUID.randomUUID();
    // Note: in our case, policies have a 1:1 relationship with roles
    final List<IamRole> allPolicies =
        List.of(IamRole.ADMIN, IamRole.STEWARD, IamRole.READER, IamRole.DISCOVERER);
    final List<IamRole> syncedPolicies = List.of(IamRole.STEWARD, IamRole.READER);

    for (IamRole policy : allPolicies) {
      when(samGoogleApi.syncPolicy(
              IamResourceType.DATASNAPSHOT.getSamResourceName(),
              snapshotId.toString(),
              policy.toString()))
          .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", List.of()));
    }

    assertThat(
        samIam.createSnapshotResource(userReq, snapshotId, List.of()),
        is(
            syncedPolicies.stream()
                .collect(Collectors.toMap(p -> p, p -> "policygroup-" + p + "@firecloud.org"))));
  }

  @Test
  public void testCreateProfile() throws InterruptedException, ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID profileId = UUID.randomUUID();
    // Note: in our case, policies have a 1:1 relationship with roles
    final List<IamRole> allPolicies = List.of(IamRole.ADMIN, IamRole.OWNER, IamRole.USER);

    for (IamRole policy : allPolicies) {
      when(samGoogleApi.syncPolicy(
              IamResourceType.DATASNAPSHOT.getSamResourceName(),
              profileId.toString(),
              policy.toString()))
          .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", List.of()));
    }

    samIam.createProfileResource(userReq, profileId.toString());
  }

  @Test
  public void testAddPolicy() throws InterruptedException, ApiException {
    final UUID id = UUID.randomUUID();
    final String userEmail = "a@a.com";
    when(samResourceApi.getPolicyV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString()))
        .thenReturn(new AccessPolicyMembershipV2().memberEmails(List.of(userEmail)));
    final PolicyModel policyModel =
        samIam.addPolicyMember(
            userReq, IamResourceType.SPEND_PROFILE, id, IamRole.OWNER.toString(), userEmail);
    assertThat(
        policyModel,
        is(new PolicyModel().name(IamRole.OWNER.toString()).addMembersItem(userEmail)));
    verify(samResourceApi, times(1))
        .addUserToPolicyV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString(),
            userEmail);
  }

  @Test
  public void testDeletePolicy() throws InterruptedException, ApiException {
    final UUID id = UUID.randomUUID();
    final String userEmail = "a@a.com";
    when(samResourceApi.getPolicyV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString()))
        .thenReturn(new AccessPolicyMembershipV2().memberEmails(List.of()));
    final PolicyModel policyModel =
        samIam.deletePolicyMember(
            userReq, IamResourceType.SPEND_PROFILE, id, IamRole.OWNER.toString(), userEmail);
    assertThat(
        policyModel, is(new PolicyModel().name(IamRole.OWNER.toString()).members(List.of())));
    verify(samResourceApi, times(1))
        .removeUserFromPolicyV2(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString(),
            userEmail);
  }

  private void mockUserInfo(String userSubjectId, String userEmail) throws ApiException {
    when(samUsersApi.getUserStatusInfo())
        .thenReturn(
            new org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo()
                .userSubjectId(userSubjectId)
                .userEmail(userEmail)
                .enabled(true));
  }

  @Test
  public void listAuthorizedResourcesTest() throws Exception {
    UUID id = UUID.randomUUID();
    when(samResourceApi.listResourcesAndPoliciesV2(
            IamResourceType.DATASNAPSHOT.getSamResourceName()))
        .thenReturn(
            List.of(
                new UserResourcesResponse()
                    .resourceId(id.toString())
                    .direct(new RolesAndActions().roles(List.of(IamRole.OWNER.toString()))),
                new UserResourcesResponse()
                    .resourceId(id.toString())
                    .direct(new RolesAndActions().roles(List.of(IamRole.READER.toString())))));
    Map<UUID, Set<IamRole>> uuidSetMap =
        samIam.listAuthorizedResources(userReq, IamResourceType.DATASNAPSHOT);
    assertThat(uuidSetMap, is((Map.of(id, Set.of(IamRole.OWNER, IamRole.READER)))));
  }

  @Test(expected = IamUnauthorizedException.class)
  public void listAuthorizedResourcesTest401Error() throws Exception {
    when(samResourceApi.listResourcesAndPoliciesV2(
            IamResourceType.DATASNAPSHOT.getSamResourceName()))
        .thenThrow(IamUnauthorizedException.class);
    try {
      samIam.listAuthorizedResources(userReq, IamResourceType.DATASNAPSHOT);
    } finally {
      verify(samResourceApi, times(1))
          .listResourcesAndPoliciesV2(IamResourceType.DATASNAPSHOT.getSamResourceName());
    }
  }
}
