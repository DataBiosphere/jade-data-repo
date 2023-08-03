package bio.terra.service.auth.iam.sam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.PolicyModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import com.google.api.client.http.HttpStatusCodes;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.GroupApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipV2;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntryV2;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.broadinstitute.dsde.workbench.client.sam.model.RolesAndActions;
import org.broadinstitute.dsde.workbench.client.sam.model.SignedUrlRequest;
import org.broadinstitute.dsde.workbench.client.sam.model.SubsystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.model.UserInfo;
import org.broadinstitute.dsde.workbench.client.sam.model.UserResourcesResponse;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class SamIamTest {

  @Mock private SamApiService samApiService;
  @Mock private GoogleApi samGoogleApi;
  @Mock private UsersApi samUsersApi;

  private SamIam samIam;
  private static final String ADMIN_EMAIL = "samAdminGroupEmail@a.com";
  private final SamConfiguration samConfig =
      new SamConfiguration(
          "https://sam.dsde-dev.broadinstitute.org",
          "samStewardsGroupEmail@a.com",
          ADMIN_EMAIL,
          10,
          30,
          60);
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() throws Exception {
    GoogleResourceConfiguration resourceConfiguration =
        new GoogleResourceConfiguration("jade-data-repo", 600, 4, false, "123456");
    samIam =
        new SamIam(
            samConfig,
            new ConfigurationService(
                samConfig, null, resourceConfiguration, new ApplicationConfiguration()),
            samApiService);
  }

  private void mockSamGoogleApi() {
    when(samApiService.googleApi(TEST_USER.getToken())).thenReturn(samGoogleApi);
  }

  private void mockSamUsersApi() {
    when(samApiService.usersApi(TEST_USER.getToken())).thenReturn(samUsersApi);
  }

  private void mockUserInfo(String userSubjectId, String userEmail) throws ApiException {
    mockSamUsersApi();
    when(samUsersApi.getUserStatusInfo())
        .thenReturn(
            new org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo()
                .userSubjectId(userSubjectId)
                .userEmail(userEmail)
                .enabled(true));
  }

  @Test
  void testExtractErrorMessageSimple() {
    ErrorReport errorReport = new ErrorReport().message("FOO").source("sam");

    assertThat(SamIam.extractErrorMessage(errorReport), is("FOO"));
  }

  @Test
  void testExtractErrorMessageSimpleNested() {
    ErrorReport errorReport =
        new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport().message("BAR").source("sam"));

    assertThat(SamIam.extractErrorMessage(errorReport), is("FOO: BAR"));
  }

  @Test
  void testExtractErrorMessageDeepNested() {
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
  void testGetUserInfo() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    assertThat(
        samIam.getUserInfo(TEST_USER),
        is(new UserStatusInfo().userSubjectId(userSubjectId).userEmail(userEmail).enabled(true)));
  }

  @Test
  void testCreateDatasetResourceRequestWithPolicySpecifications() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID datasetId = UUID.randomUUID();

    String stewardEmail1 = "steward1@a.com";
    String stewardEmail2 = "steward3@a.com";
    String custodianEmail = "custodian@a.com";
    String snapshotCreatorEmail = "snapshotCreator@a.com";
    DatasetRequestModelPolicies policySpecs =
        new DatasetRequestModelPolicies()
            .addStewardsItem(stewardEmail1)
            .addStewardsItem(stewardEmail2)
            .addCustodiansItem(custodianEmail)
            .addSnapshotCreatorsItem(snapshotCreatorEmail);
    CreateResourceRequestV2 req =
        samIam.createDatasetResourceRequest(TEST_USER, datasetId, policySpecs);

    assertThat(req.getResourceId(), is(datasetId.toString()));

    List<IamRole> policyKeys = req.getPolicies().keySet().stream().map(IamRole::fromValue).toList();
    assertThat(
        policyKeys,
        containsInAnyOrder(
            IamRole.ADMIN, IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR));

    AccessPolicyMembershipV2 admin = req.getPolicies().get(IamRole.ADMIN.toString());
    assertThat(admin.getRoles(), contains(IamRole.ADMIN.toString()));
    assertThat(admin.getMemberEmails(), contains(ADMIN_EMAIL));

    AccessPolicyMembershipV2 steward = req.getPolicies().get(IamRole.STEWARD.toString());
    assertThat(steward.getRoles(), contains(IamRole.STEWARD.toString()));
    assertThat(steward.getMemberEmails(), contains(userEmail, stewardEmail1, stewardEmail2));

    AccessPolicyMembershipV2 custodian = req.getPolicies().get(IamRole.CUSTODIAN.toString());
    assertThat(custodian.getRoles(), contains(IamRole.CUSTODIAN.toString()));
    assertThat(custodian.getMemberEmails(), contains(custodianEmail));

    AccessPolicyMembershipV2 snapshotCreator =
        req.getPolicies().get(IamRole.SNAPSHOT_CREATOR.toString());
    assertThat(snapshotCreator.getRoles(), contains(IamRole.SNAPSHOT_CREATOR.toString()));
    assertThat(snapshotCreator.getMemberEmails(), contains(snapshotCreatorEmail));
  }

  @Test
  void testCreateSnapshotResourceRequestWithoutPolicySpecifications() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID snapshotId = UUID.randomUUID();

    CreateResourceRequestV2 reqNullPolicies =
        samIam.createSnapshotResourceRequest(TEST_USER, snapshotId, null);
    CreateResourceRequestV2 reqEmptyPolicies =
        samIam.createSnapshotResourceRequest(
            TEST_USER, snapshotId, new SnapshotRequestModelPolicies());

    for (CreateResourceRequestV2 req : List.of(reqNullPolicies, reqEmptyPolicies)) {
      assertThat(req.getResourceId(), is(snapshotId.toString()));

      List<IamRole> policyKeys =
          req.getPolicies().keySet().stream().map(IamRole::fromValue).toList();
      assertThat(
          policyKeys,
          containsInAnyOrder(IamRole.ADMIN, IamRole.STEWARD, IamRole.READER, IamRole.DISCOVERER));

      AccessPolicyMembershipV2 admin = req.getPolicies().get(IamRole.ADMIN.toString());
      assertThat(admin.getRoles(), contains(IamRole.ADMIN.toString()));
      assertThat(admin.getMemberEmails(), contains(ADMIN_EMAIL));

      AccessPolicyMembershipV2 steward = req.getPolicies().get(IamRole.STEWARD.toString());
      assertThat(steward.getRoles(), contains(IamRole.STEWARD.toString()));
      assertThat(steward.getMemberEmails(), contains(userEmail));

      AccessPolicyMembershipV2 reader = req.getPolicies().get(IamRole.READER.toString());
      assertThat(reader.getRoles(), contains(IamRole.READER.toString()));
      assertThat(reader.getMemberEmails(), empty());

      AccessPolicyMembershipV2 discoverer = req.getPolicies().get(IamRole.DISCOVERER.toString());
      assertThat(discoverer.getRoles(), contains(IamRole.DISCOVERER.toString()));
      assertThat(discoverer.getMemberEmails(), empty());
    }
  }

  @Test
  void testCreateSnapshotResourceRequestWithPolicySpecifications() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID snapshotId = UUID.randomUUID();

    String stewardEmail1 = "steward1@a.com";
    String stewardEmail2 = "steward3@a.com";
    String readerEmail = "reader@a.com";
    String discovererEmail = "discoverer@a.com";
    SnapshotRequestModelPolicies policySpecs =
        new SnapshotRequestModelPolicies()
            .addStewardsItem(stewardEmail1)
            .addStewardsItem(stewardEmail2)
            .addReadersItem(readerEmail)
            .addDiscoverersItem(discovererEmail);
    CreateResourceRequestV2 req =
        samIam.createSnapshotResourceRequest(TEST_USER, snapshotId, policySpecs);

    assertThat(req.getResourceId(), is(snapshotId.toString()));

    List<IamRole> policyKeys = req.getPolicies().keySet().stream().map(IamRole::fromValue).toList();
    assertThat(
        policyKeys,
        containsInAnyOrder(IamRole.ADMIN, IamRole.STEWARD, IamRole.READER, IamRole.DISCOVERER));

    AccessPolicyMembershipV2 admin = req.getPolicies().get(IamRole.ADMIN.toString());
    assertThat(admin.getRoles(), contains(IamRole.ADMIN.toString()));
    assertThat(admin.getMemberEmails(), contains(ADMIN_EMAIL));

    AccessPolicyMembershipV2 steward = req.getPolicies().get(IamRole.STEWARD.toString());
    assertThat(steward.getRoles(), contains(IamRole.STEWARD.toString()));
    assertThat(steward.getMemberEmails(), contains(userEmail, stewardEmail1, stewardEmail2));

    AccessPolicyMembershipV2 reader = req.getPolicies().get(IamRole.READER.toString());
    assertThat(reader.getRoles(), contains(IamRole.READER.toString()));
    assertThat(reader.getMemberEmails(), contains(readerEmail));

    AccessPolicyMembershipV2 discoverer = req.getPolicies().get(IamRole.DISCOVERER.toString());
    assertThat(discoverer.getRoles(), contains(IamRole.DISCOVERER.toString()));
    assertThat(discoverer.getMemberEmails(), contains(discovererEmail));
  }

  @Test
  void testCreateDatasetResourceRequestWithoutPolicySpecifications() throws ApiException {
    final String userSubjectId = "userid";
    final String userEmail = "a@a.com";
    mockUserInfo(userSubjectId, userEmail);

    final UUID datasetId = UUID.randomUUID();

    CreateResourceRequestV2 reqNullPolicies =
        samIam.createDatasetResourceRequest(TEST_USER, datasetId, null);
    CreateResourceRequestV2 reqEmptyPolicies =
        samIam.createDatasetResourceRequest(
            TEST_USER, datasetId, new DatasetRequestModelPolicies());

    for (CreateResourceRequestV2 req : List.of(reqNullPolicies, reqEmptyPolicies)) {
      assertThat(req.getResourceId(), is(datasetId.toString()));

      List<IamRole> policyKeys =
          req.getPolicies().keySet().stream().map(IamRole::fromValue).toList();
      assertThat(
          policyKeys,
          containsInAnyOrder(
              IamRole.ADMIN, IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR));

      AccessPolicyMembershipV2 admin = req.getPolicies().get(IamRole.ADMIN.toString());
      assertThat(admin.getRoles(), contains(IamRole.ADMIN.toString()));
      assertThat(admin.getMemberEmails(), contains(ADMIN_EMAIL));

      AccessPolicyMembershipV2 steward = req.getPolicies().get(IamRole.STEWARD.toString());
      assertThat(steward.getRoles(), contains(IamRole.STEWARD.toString()));
      assertThat(steward.getMemberEmails(), contains(userEmail));

      AccessPolicyMembershipV2 custodian = req.getPolicies().get(IamRole.CUSTODIAN.toString());
      assertThat(custodian.getRoles(), contains(IamRole.CUSTODIAN.toString()));
      assertThat(custodian.getMemberEmails(), empty());

      AccessPolicyMembershipV2 snapshotCreator =
          req.getPolicies().get(IamRole.SNAPSHOT_CREATOR.toString());
      assertThat(snapshotCreator.getRoles(), contains(IamRole.SNAPSHOT_CREATOR.toString()));
      assertThat(snapshotCreator.getMemberEmails(), empty());
    }
  }

  @Test
  void testRegisterUser() throws InterruptedException, ApiException {
    mockSamUsersApi();
    UserStatus userStatus =
        new UserStatus()
            .userInfo(
                new UserInfo()
                    .userEmail("tdr-ingest-sa@my-project.iam.gserviceaccount.com")
                    .userSubjectId("subid"));
    when(samUsersApi.createUserV2(null)).thenReturn(userStatus);
    samIam.registerUser(TEST_USER.getToken());
  }

  @Test
  void testSignUrl() throws InterruptedException, ApiException {
    mockSamGoogleApi();
    String project = "myProject";
    String path = "gs://bucket/path/to/file";
    Duration duration = Duration.ofMinutes(15);
    samIam.signUrlForBlob(TEST_USER, project, path, duration);

    // Verify the arguments are properly parsed and passed through
    verify(samGoogleApi)
        .getSignedUrlForBlob(
            project,
            new SignedUrlRequest()
                .bucketName("bucket")
                .blobName("path/to/file")
                .duration(BigDecimal.valueOf(15)));
  }

  @Nested
  class TestResourcesApi {

    @Mock private ResourcesApi samResourceApi;

    @BeforeEach
    void setUp() throws Exception {
      when(samApiService.resourcesApi(TEST_USER.getToken())).thenReturn(samResourceApi);
    }

    @Test
    void testIgnoresNonUUIDResourceName() throws ApiException, InterruptedException {
      final UUID goodId = UUID.randomUUID();
      final String badId = "badUUID";
      when(samResourceApi.listResourcesAndPoliciesV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName()))
          .thenReturn(
              List.of(
                  new UserResourcesResponse().resourceId(goodId.toString()),
                  new UserResourcesResponse().resourceId(badId)));

      Set<UUID> uuids =
          samIam.listAuthorizedResources(TEST_USER, IamResourceType.SPEND_PROFILE).keySet();
      assertThat(uuids, contains(goodId));
    }

    @Test
    void testAuthorization() throws ApiException, InterruptedException {
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
          samIam.isAuthorized(
              TEST_USER, IamResourceType.SPEND_PROFILE, "my-id", IamAction.READ_DATA));
      assertFalse(
          samIam.isAuthorized(
              TEST_USER, IamResourceType.SPEND_PROFILE, "my-id", IamAction.ALTER_POLICIES));
    }

    @Test
    void testHasAnyActions() throws ApiException, InterruptedException {
      when(samResourceApi.resourceActionsV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(), "my-id-1"))
          .thenReturn(List.of(IamAction.READ_DATA.toString()));
      when(samResourceApi.resourceActionsV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(), "my-id-2"))
          .thenReturn(List.of());
      assertTrue(samIam.hasAnyActions(TEST_USER, IamResourceType.SPEND_PROFILE, "my-id-1"));
      assertFalse(samIam.hasAnyActions(TEST_USER, IamResourceType.SPEND_PROFILE, "my-id-2"));
    }

    @Test
    void testDeleteResource() throws InterruptedException, ApiException {
      final UUID datasetId = UUID.randomUUID();
      samIam.deleteDatasetResource(TEST_USER, datasetId);
      // Verify that the correct Sam API call was made
      verify(samResourceApi, times(1))
          .deleteResourceV2(IamResourceType.DATASET.getSamResourceName(), datasetId.toString());

      final UUID snapshotId = UUID.randomUUID();
      samIam.deleteSnapshotResource(TEST_USER, snapshotId);
      verify(samResourceApi, times(1))
          .deleteResourceV2(
              IamResourceType.DATASNAPSHOT.getSamResourceName(), snapshotId.toString());

      final UUID profileId = UUID.randomUUID();
      samIam.deleteProfileResource(TEST_USER, profileId.toString());
      verify(samResourceApi, times(1))
          .deleteResourceV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(), profileId.toString());
    }

    @Test
    void testRetrievePoliciesAndEmails() throws ApiException, InterruptedException {
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
          samIam.retrievePolicies(TEST_USER, IamResourceType.SPEND_PROFILE, id),
          is(
              List.of(
                  new SamPolicyModel()
                      .name(IamRole.CUSTODIAN.toString())
                      .addMembersItem(memberEmail)
                      .memberPolicies(List.of()))));

      assertThat(
          samIam.retrievePolicyEmails(TEST_USER, IamResourceType.SPEND_PROFILE, id),
          is(Map.of(IamRole.CUSTODIAN, policyEmail)));
    }

    @Test
    void testCreateDataset() throws InterruptedException, ApiException {
      mockSamGoogleApi();
      final String userSubjectId = "userid";
      final String userEmail = "a@a.com";
      mockUserInfo(userSubjectId, userEmail);

      final UUID datasetId = UUID.randomUUID();
      // Note: in our case, policies have a 1:1 relationship with roles
      final List<IamRole> syncedPolicies =
          List.of(IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR);

      for (IamRole policy : syncedPolicies) {
        when(samGoogleApi.syncPolicy(
                IamResourceType.DATASET.getSamResourceName(),
                datasetId.toString(),
                policy.toString(),
                null))
            .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", List.of()));
      }

      assertThat(
          samIam.createDatasetResource(TEST_USER, datasetId, null),
          is(
              syncedPolicies.stream()
                  .collect(Collectors.toMap(p -> p, p -> "policygroup-" + p + "@firecloud.org"))));
    }

    @Test
    void testCreateSnapshot() throws InterruptedException, ApiException {
      mockSamGoogleApi();
      final String userSubjectId = "userid";
      final String userEmail = "a@a.com";
      mockUserInfo(userSubjectId, userEmail);

      final UUID snapshotId = UUID.randomUUID();
      // Note: in our case, policies have a 1:1 relationship with roles
      final List<IamRole> syncedPolicies = List.of(IamRole.STEWARD, IamRole.READER);

      for (IamRole policy : syncedPolicies) {
        when(samGoogleApi.syncPolicy(
                IamResourceType.DATASNAPSHOT.getSamResourceName(),
                snapshotId.toString(),
                policy.toString(),
                null))
            .thenReturn(Map.of("policygroup-" + policy + "@firecloud.org", List.of()));
      }

      assertThat(
          samIam.createSnapshotResource(TEST_USER, snapshotId, null),
          is(
              syncedPolicies.stream()
                  .collect(Collectors.toMap(p -> p, p -> "policygroup-" + p + "@firecloud.org"))));
    }

    @Test
    void testCreateProfile() throws InterruptedException, ApiException {
      final UUID profileId = UUID.randomUUID();
      final String userSubjectId = "userid";
      final String userEmail = "a@a.com";
      mockUserInfo(userSubjectId, userEmail);

      CreateResourceRequestV2 req = new CreateResourceRequestV2();
      req.setResourceId(profileId.toString());
      req.putPoliciesItem(
          IamRole.ADMIN.toString(),
          new AccessPolicyMembershipV2()
              .memberEmails(List.of(samConfig.adminsGroupEmail()))
              .roles(List.of(IamRole.ADMIN.toString())));
      req.putPoliciesItem(
          IamRole.OWNER.toString(),
          new AccessPolicyMembershipV2()
              .memberEmails(List.of(userEmail))
              .roles(List.of(IamRole.OWNER.toString())));
      req.putPoliciesItem(
          IamRole.USER.toString(),
          new AccessPolicyMembershipV2().roles(List.of(IamRole.USER.toString())));
      req.authDomain(List.of());
      samIam.createProfileResource(TEST_USER, profileId.toString());
      verify(samResourceApi).createResourceV2(IamResourceType.SPEND_PROFILE.toString(), req);
    }

    @Test
    void testAddPolicy() throws InterruptedException, ApiException {
      final UUID id = UUID.randomUUID();
      final String userEmail = "a@a.com";
      when(samResourceApi.getPolicyV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(),
              id.toString(),
              IamRole.OWNER.toString()))
          .thenReturn(new AccessPolicyMembershipV2().memberEmails(List.of(userEmail)));
      final PolicyModel policyModel =
          samIam.addPolicyMember(
              TEST_USER, IamResourceType.SPEND_PROFILE, id, IamRole.OWNER.toString(), userEmail);
      assertThat(
          policyModel,
          is(new PolicyModel().name(IamRole.OWNER.toString()).addMembersItem(userEmail)));
      verify(samResourceApi, times(1))
          .addUserToPolicyV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(),
              id.toString(),
              IamRole.OWNER.toString(),
              userEmail,
              null);
    }

    @Test
    void testDeletePolicy() throws InterruptedException, ApiException {
      final UUID id = UUID.randomUUID();
      final String userEmail = "a@a.com";
      when(samResourceApi.getPolicyV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(),
              id.toString(),
              IamRole.OWNER.toString()))
          .thenReturn(new AccessPolicyMembershipV2().memberEmails(List.of()));
      final PolicyModel policyModel =
          samIam.deletePolicyMember(
              TEST_USER, IamResourceType.SPEND_PROFILE, id, IamRole.OWNER.toString(), userEmail);
      assertThat(
          policyModel, is(new PolicyModel().name(IamRole.OWNER.toString()).members(List.of())));
      verify(samResourceApi, times(1))
          .removeUserFromPolicyV2(
              IamResourceType.SPEND_PROFILE.getSamResourceName(),
              id.toString(),
              IamRole.OWNER.toString(),
              userEmail);
    }

    @Test
    void listAuthorizedResourcesTest() throws Exception {
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
          samIam.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT);
      assertThat(uuidSetMap, is((Map.of(id, Set.of(IamRole.OWNER, IamRole.READER)))));
    }

    @Test
    void listAuthorizedResourcesTest401Error() throws Exception {
      when(samResourceApi.listResourcesAndPoliciesV2(
              IamResourceType.DATASNAPSHOT.getSamResourceName()))
          .thenThrow(IamUnauthorizedException.class);
      assertThrows(
          IamUnauthorizedException.class,
          () -> {
            samIam.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT);
          });
    }
  }

  @Nested
  class TestStatusApi {

    @Mock private StatusApi samStatusApi;

    @BeforeEach
    void setUp() throws Exception {
      when(samApiService.statusApi()).thenReturn(samStatusApi);
    }

    @Test
    void testGetStatus() throws ApiException {
      when(samStatusApi.getSystemStatus())
          .thenReturn(
              new SystemStatus()
                  .ok(true)
                  .systems(Map.of("GooglePubSub", new SubsystemStatus().ok(true))));
      assertThat(
          samIam.samStatus(),
          is(
              new RepositoryStatusModelSystems()
                  .ok(true)
                  .message(
                      """
                            {GooglePubSub=class SubsystemStatus {
                                ok: true
                                messages: null
                            }}""")));
    }

    @Test
    void testGetStatusException() throws ApiException {
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
  }

  @Nested
  class TestGroupApi {

    @Mock private GroupApi samGroupApi;

    @BeforeEach
    void setUp() throws Exception {
      when(samApiService.groupApi(TEST_USER.getToken())).thenReturn(samGroupApi);
    }

    @Test
    void testCreateGroup() throws ApiException, InterruptedException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";
      String groupEmail = String.format("%s@dev.test.firecloud.org", groupName);

      when(samGroupApi.getGroup(groupName)).thenReturn(groupEmail);
      assertThat(
          "Firecloud group email is returned when creation succeeds and email returned by SAM",
          samIam.createGroup(accessToken, groupName),
          equalTo(groupEmail));
      verify(samGroupApi).postGroup(groupName, null);
    }

    @Test
    void testCreateGroupWithCreationFailure() throws ApiException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";

      ApiException samEx =
          new ApiException(HttpStatusCodes.STATUS_CODE_CONFLICT, "Group already exists");
      doThrow(samEx).when(samGroupApi).postGroup(groupName, null);
      assertThrows(IamConflictException.class, () -> samIam.createGroup(accessToken, groupName));
      verify(samGroupApi, never()).getGroup(groupName);
    }

    @Test
    void testCreateGroupWithEmailFetchFailure() throws ApiException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";

      ApiException samEx =
          new ApiException(HttpStatusCodes.STATUS_CODE_NOT_FOUND, "Group not found");
      when(samGroupApi.getGroup(groupName)).thenThrow(samEx);
      assertThrows(IamNotFoundException.class, () -> samIam.createGroup(accessToken, groupName));
      verify(samGroupApi).postGroup(groupName, null);
    }

    @Test
    void testOverwriteGroupPolicyEmails() throws InterruptedException, ApiException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";
      String policyName = IamRole.MEMBER.toString();
      List<String> emails = List.of("user@a.com");

      samIam.overwriteGroupPolicyEmails(accessToken, groupName, policyName, emails);
      verify(samGroupApi).overwriteGroupPolicyEmails(groupName, policyName, emails);
    }

    @Test
    void testOverwriteGroupPolicyEmailsThrowsWhenSamGroupApiThrows() throws ApiException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";
      String policyName = IamRole.MEMBER.toString();
      List<String> emails = List.of("user@a.com");

      ApiException samEx =
          new ApiException(HttpStatusCodes.STATUS_CODE_NOT_FOUND, "Group not found");
      doThrow(samEx).when(samGroupApi).overwriteGroupPolicyEmails(groupName, policyName, emails);
      assertThrows(
          IamNotFoundException.class,
          () -> samIam.overwriteGroupPolicyEmails(accessToken, groupName, policyName, emails));
    }

    @Test
    void testDeleteGroup() throws ApiException, InterruptedException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";

      samIam.deleteGroup(accessToken, groupName);
      verify(samGroupApi).deleteGroup(groupName);
    }

    @Test
    void testDeleteGroupThrowsWhenSamGroupApiThrows() throws ApiException {
      String accessToken = TEST_USER.getToken();
      String groupName = "firecloud_group_name";

      ApiException samEx =
          new ApiException(HttpStatusCodes.STATUS_CODE_NOT_FOUND, "Group not found");
      doThrow(samEx).when(samGroupApi).deleteGroup(groupName);
      assertThrows(IamNotFoundException.class, () -> samIam.deleteGroup(accessToken, groupName));
    }
  }
}
