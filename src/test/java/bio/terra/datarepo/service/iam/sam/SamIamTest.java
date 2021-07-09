package bio.terra.datarepo.service.iam.sam;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.app.configuration.SamConfiguration;
import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.model.PolicyModel;
import bio.terra.datarepo.model.RepositoryStatusModelSystems;
import bio.terra.datarepo.model.UserStatusInfo;
import bio.terra.datarepo.service.configuration.ConfigEnum;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequest;
import bio.terra.datarepo.service.iam.IamAction;
import bio.terra.datarepo.service.iam.IamResourceType;
import bio.terra.datarepo.service.iam.IamRole;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembership;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntry;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
    when(userReq.getRequiredToken()).thenReturn(userToken);
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

    assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO");
  }

  @Test
  public void testExtractErrorMessageSimpleNested() {
    ErrorReport errorReport =
        new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport().message("BAR").source("sam"));

    assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO: BAR");
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

    assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO: BAR: (BAZ1, BAZ2: QUX)");
  }

  @Test
  public void testIgnoresNonUUIDResourceName() throws ApiException, InterruptedException {
    final String goodId = UUID.randomUUID().toString();
    final String badId = "badUUID";
    when(samResourceApi.listResourcesAndPolicies(
            IamResourceType.SPEND_PROFILE.getSamResourceName()))
        .thenReturn(
            List.of(
                new ResourceAndAccessPolicy().resourceId(goodId),
                new ResourceAndAccessPolicy().resourceId(badId)));

    List<UUID> uuids = samIam.listAuthorizedResources(userReq, IamResourceType.SPEND_PROFILE);
    assertThat(uuids).containsExactly(UUID.fromString(goodId));
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
    assertThat(
            samIam.isAuthorized(
                userReq, IamResourceType.SPEND_PROFILE, "my-id", IamAction.READ_DATA))
        .isTrue();
    assertThat(
            samIam.isAuthorized(
                userReq, IamResourceType.SPEND_PROFILE, "my-id", IamAction.ALTER_POLICIES))
        .isFalse();
  }

  @Test
  public void testGetStatus() throws ApiException {
    when(samStatusApi.getSystemStatus())
        .thenReturn(
            new SystemStatus().ok(true).systems(Map.of("GooglePubSub", Map.of("ok", true))));
    assertThat(samIam.samStatus())
        .isEqualTo(new RepositoryStatusModelSystems().ok(true).message("{GooglePubSub={ok=true}}"));
  }

  @Test
  public void testGetStatusException() throws ApiException {
    when(samStatusApi.getSystemStatus()).thenThrow(new ApiException("BOOM!"));
    assertThat(samIam.samStatus())
        .isEqualTo(
            new RepositoryStatusModelSystems()
                .ok(false)
                .message(
                    "Sam status check failed: bio.terra.datarepo.service.iam.exception.IamInternalServerErrorException: "
                        + "BOOM!"));
  }

  @Test
  public void testGetUserInfo() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    when(samUsersApi.getUserStatusInfo())
        .thenReturn(
            new org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo()
                .userSubjectId(userSubjectId)
                .userEmail(userEmail)
                .enabled(true));

    assertThat(samIam.getUserInfo(userReq))
        .isEqualTo(
            new UserStatusInfo().userSubjectId(userSubjectId).userEmail(userEmail).enabled(true));
  }

  @Test
  public void testHasActions() throws ApiException, InterruptedException {
    when(samResourceApi.resourceActions(
            IamResourceType.SPEND_PROFILE.getSamResourceName(), "my-id-1"))
        .thenReturn(Collections.singletonList(IamAction.READ_DATA.toString()));
    when(samResourceApi.resourceActions(
            IamResourceType.SPEND_PROFILE.getSamResourceName(), "my-id-2"))
        .thenReturn(Collections.emptyList());
    assertThat(samIam.hasActions(userReq, IamResourceType.SPEND_PROFILE, "my-id-1")).isTrue();
    assertThat(samIam.hasActions(userReq, IamResourceType.SPEND_PROFILE, "my-id-2")).isFalse();
  }

  @Test
  public void testDeleteResource() throws InterruptedException, ApiException {
    final UUID datasetId = UUID.randomUUID();
    samIam.deleteDatasetResource(userReq, datasetId);
    // Verify that the correct Sam API call was made
    verify(samResourceApi, times(1))
        .deleteResource(eq(IamResourceType.DATASET.getSamResourceName()), eq(datasetId.toString()));

    final UUID snapshotId = UUID.randomUUID();
    samIam.deleteSnapshotResource(userReq, snapshotId);
    verify(samResourceApi, times(1))
        .deleteResource(
            eq(IamResourceType.DATASNAPSHOT.getSamResourceName()), eq(snapshotId.toString()));

    final UUID profileId = UUID.randomUUID();
    samIam.deleteProfileResource(userReq, profileId.toString());
    verify(samResourceApi, times(1))
        .deleteResource(
            eq(IamResourceType.SPEND_PROFILE.getSamResourceName()), eq(profileId.toString()));
  }

  @Test
  public void testRetrievePoliciesAndEmails() throws ApiException, InterruptedException {
    final UUID id = UUID.randomUUID();
    final String policyEmail = "policygroup@firecloud.org";
    final String memberEmail = "a@a.com";
    when(samResourceApi.listResourcePolicies(
            IamResourceType.SPEND_PROFILE.getSamResourceName(), id.toString()))
        .thenReturn(
            Collections.singletonList(
                new AccessPolicyResponseEntry()
                    .policyName(IamRole.CUSTODIAN.toString())
                    .email(policyEmail)
                    .policy(new AccessPolicyMembership().addMemberEmailsItem(memberEmail))));

    assertThat(samIam.retrievePolicies(userReq, IamResourceType.SPEND_PROFILE, id))
        .isEqualTo(
            Collections.singletonList(
                new PolicyModel().name(IamRole.CUSTODIAN.toString()).addMembersItem(memberEmail)));

    assertThat(samIam.retrievePolicyEmails(userReq, IamResourceType.SPEND_PROFILE, id))
        .isEqualTo(Map.of(IamRole.CUSTODIAN, policyEmail));
  }

  @Test
  public void testCreateDataset() throws InterruptedException, ApiException {
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
          .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", Collections.emptyList()));
    }

    assertThat(samIam.createDatasetResource(userReq, datasetId))
        .isEqualTo(
            syncedPolicies.stream()
                .collect(Collectors.toMap(p -> p, p -> "policygroup-" + p + "@firecloud.org")));
  }

  @Test
  public void testCreateSnapshot() throws InterruptedException, ApiException {
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
          .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", Collections.emptyList()));
    }

    assertThat(samIam.createSnapshotResource(userReq, snapshotId, Collections.emptyList()))
        .isEqualTo(
            syncedPolicies.stream()
                .collect(Collectors.toMap(p -> p, p -> "policygroup-" + p + "@firecloud.org")));
  }

  @Test
  public void testCreateProfile() throws InterruptedException, ApiException {
    final UUID profileId = UUID.randomUUID();
    // Note: in our case, policies have a 1:1 relationship with roles
    final List<IamRole> allPolicies = List.of(IamRole.ADMIN, IamRole.OWNER, IamRole.USER);

    for (IamRole policy : allPolicies) {
      when(samGoogleApi.syncPolicy(
              IamResourceType.DATASNAPSHOT.getSamResourceName(),
              profileId.toString(),
              policy.toString()))
          .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", Collections.emptyList()));
    }

    samIam.createProfileResource(userReq, profileId.toString());
  }

  @Test
  public void testAddPolicy() throws InterruptedException, ApiException {
    final UUID id = UUID.randomUUID();
    final String userEmail = "a@a.com";
    when(samResourceApi.getPolicy(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString()))
        .thenReturn(
            new AccessPolicyMembership().memberEmails(Collections.singletonList(userEmail)));
    final PolicyModel policyModel =
        samIam.addPolicyMember(
            userReq, IamResourceType.SPEND_PROFILE, id, IamRole.OWNER.toString(), userEmail);
    assertThat(policyModel)
        .isEqualTo(new PolicyModel().name(IamRole.OWNER.toString()).addMembersItem(userEmail));
    verify(samResourceApi, times(1))
        .addUserToPolicy(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString(),
            userEmail);
  }

  @Test
  public void testDeletePolicy() throws InterruptedException, ApiException {
    final UUID id = UUID.randomUUID();
    final String userEmail = "a@a.com";
    when(samResourceApi.getPolicy(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString()))
        .thenReturn(new AccessPolicyMembership().memberEmails(Collections.emptyList()));
    final PolicyModel policyModel =
        samIam.deletePolicyMember(
            userReq, IamResourceType.SPEND_PROFILE, id, IamRole.OWNER.toString(), userEmail);
    assertThat(policyModel)
        .isEqualTo(
            new PolicyModel().name(IamRole.OWNER.toString()).members(Collections.emptyList()));
    verify(samResourceApi, times(1))
        .removeUserFromPolicy(
            IamResourceType.SPEND_PROFILE.getSamResourceName(),
            id.toString(),
            IamRole.OWNER.toString(),
            userEmail);
  }
}
