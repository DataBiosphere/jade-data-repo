package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.oauth2.GoogleCredentialsService;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.journal.JournalService;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class IamServiceTest {
  private static final UUID ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final List<String> AUTH_DOMAIN = List.of("group1", "group2");

  @Mock private IamProviderInterface iamProvider;

  @Mock private ConfigurationService configurationService;

  @Mock private GoogleCredentialsService googleCredentialsService;

  private IamService iamService;

  @BeforeEach
  void setup() {
    when(configurationService.getParameterValue(ConfigEnum.AUTH_CACHE_TIMEOUT_SECONDS))
        .thenReturn(0);

    iamService =
        new IamService(
            iamProvider,
            configurationService,
            mock(JournalService.class),
            googleCredentialsService);
  }

  @Test
  void testRetrieveAuthDomain() throws InterruptedException {
    when(iamProvider.retrieveAuthDomains(TEST_USER, IamResourceType.DATASNAPSHOT, ID))
        .thenReturn(AUTH_DOMAIN);

    List<String> result =
        iamService.retrieveAuthDomains(TEST_USER, IamResourceType.DATASNAPSHOT, ID);
    verify(iamProvider).retrieveAuthDomains(TEST_USER, IamResourceType.DATASNAPSHOT, ID);
    assertEquals(AUTH_DOMAIN, result);
  }

  @Test
  void testPathAuthDomain() throws InterruptedException {
    iamService.patchAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, ID, AUTH_DOMAIN);
    verify(iamProvider).patchAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, ID, AUTH_DOMAIN);
  }

  @Test
  void testAddPolicyMember() throws InterruptedException {
    IamRole policy = IamRole.DISCOVERER;
    String email = "email";

    iamService.addPolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policy, email);
    verify(iamProvider)
        .addPolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policy, email);
  }

  @Test
  void testDeletePolicyMember() throws InterruptedException {
    IamRole policy = IamRole.DISCOVERER;
    String email = "email";

    iamService.deletePolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policy, email);
    verify(iamProvider)
        .deletePolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policy, email);
  }

  @Test
  void retrievePolicy() throws Exception {
    IamRole policy = IamRole.DISCOVERER;
    String email = "email";

    when(iamProvider.retrievePolicies(TEST_USER, IamResourceType.SPEND_PROFILE, ID))
        .thenReturn(List.of(new SamPolicyModel().name(policy.toString()).addMembersItem(email)));
    var policyModel =
        iamService.retrievePolicy(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policy);
    assertThat(policyModel, is(new PolicyModel().name(policy.toString()).addMembersItem(email)));
  }

  @Test
  void testVerifyAuthorization() throws Exception {
    IamResourceType resourceType = IamResourceType.DATASET;
    String id = ID.toString();
    IamAction action = IamAction.READ_DATA;

    when(iamProvider.isAuthorized(TEST_USER, resourceType, id, action)).thenReturn(true);
    // Checking authorization for an action associated with the caller should not throw.
    iamService.verifyAuthorization(TEST_USER, resourceType, id, action);

    when(iamProvider.isAuthorized(TEST_USER, resourceType, id, action)).thenReturn(false);
    IamForbiddenException thrown =
        assertThrows(
            IamForbiddenException.class,
            () -> iamService.verifyAuthorization(TEST_USER, resourceType, id, action),
            "Authorization verification throws if the caller is missing the action");
    assertThat(
        "Error message reflects cause",
        thrown.getMessage(),
        containsString("does not have required action '%s'".formatted(action)));
  }

  @Test
  void testVerifyAuthorizationAnyAction() throws InterruptedException {
    IamResourceType resourceType = IamResourceType.DATASET;
    String id = ID.toString();

    when(iamProvider.hasAnyActions(TEST_USER, resourceType, id)).thenReturn(true);
    iamService.verifyAuthorization(TEST_USER, resourceType, id);

    when(iamProvider.hasAnyActions(TEST_USER, resourceType, id)).thenReturn(false);
    IamForbiddenException thrown =
        assertThrows(
            IamForbiddenException.class,
            () -> iamService.verifyAuthorization(TEST_USER, resourceType, id),
            "Authorization verification throws if the caller holds no actions");
    assertThat(
        "Error message reflects cause",
        thrown.getMessage(),
        containsString("does not hold any actions"));
  }

  @Test
  void testVerifyAuthorizations() throws Exception {
    IamResourceType resourceType = IamResourceType.DATASET;
    String id = ID.toString();

    Set<IamAction> hasActions = EnumSet.of(IamAction.MANAGE_SCHEMA, IamAction.READ_DATA);
    when(iamProvider.listActions(TEST_USER, resourceType, id))
        .thenReturn(hasActions.stream().map(IamAction::toString).toList());

    // Checking authorizations for actions associated with the caller should not throw.
    iamService.verifyAuthorizations(TEST_USER, resourceType, id, Set.of());
    iamService.verifyAuthorizations(TEST_USER, resourceType, id, hasActions);

    Set<IamAction> missingActions = EnumSet.of(IamAction.UPDATE_PASSPORT_IDENTIFIER);
    Set<IamAction> requiredActions = EnumSet.copyOf(hasActions);
    requiredActions.addAll(missingActions);

    IamForbiddenException thrown =
        assertThrows(
            IamForbiddenException.class,
            () -> iamService.verifyAuthorizations(TEST_USER, resourceType, id, requiredActions),
            "Authorization verification throws if the caller is missing a required action");
    assertThat(
        "Error message reflects cause",
        thrown.getMessage(),
        containsString("missing required actions"));
    assertThat(
        "Error details contain missing actions",
        thrown.getCauses(),
        containsInAnyOrder(missingActions.stream().map(IamAction::toString).toArray()));
  }

  @Test
  void createSnapshotResource() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    UUID parentDatasetId = UUID.randomUUID();
    SnapshotRequestModelPolicies policies = new SnapshotRequestModelPolicies();
    iamService.createSnapshotResource(TEST_USER, snapshotId, parentDatasetId, policies);
    verify(iamProvider).createSnapshotResource(TEST_USER, snapshotId, parentDatasetId, policies);
  }

  @Test
  void testDeriveSnapshotPolicies() {
    assertThat(
        "Request without policies or readers returns new policy object",
        iamService.deriveSnapshotPolicies(new SnapshotRequestModel()),
        equalTo(new SnapshotRequestModelPolicies().readers(List.of())));

    List<String> readers = List.of("reader1@email.com", "reader2@email.com");
    assertThat(
        "Request without policies but with readers returns new policy object with readers",
        iamService.deriveSnapshotPolicies(new SnapshotRequestModel().readers(readers)),
        equalTo(new SnapshotRequestModelPolicies().readers(readers)));

    SnapshotRequestModelPolicies policies =
        new SnapshotRequestModelPolicies()
            .addStewardsItem("steward@email.com")
            .addReadersItem("policyreader1@email.com")
            .addDiscoverersItem("discoverer@email.com");
    assertThat(
        "Request with policies but without readers returns provided policy object",
        iamService.deriveSnapshotPolicies(new SnapshotRequestModel().policies(policies)),
        equalTo(policies));

    List<String> expectedReaders = new ArrayList<>();
    expectedReaders.addAll(policies.getReaders());
    expectedReaders.addAll(readers);
    assertThat(
        "Request with policies and readers returns policy object with combined readers",
        iamService.deriveSnapshotPolicies(
            new SnapshotRequestModel().readers(readers).policies(policies)),
        equalTo(
            new SnapshotRequestModelPolicies()
                .stewards(policies.getStewards())
                .readers(expectedReaders)
                .discoverers(policies.getDiscoverers())));
  }

  @Test
  void testVerifyResourceTypeAdminAuthorizedWithLoggingTrue() throws InterruptedException {
    UUID id = UUID.randomUUID();
    when(iamProvider.getResourceTypeAdminPermission(
            TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION))
        .thenReturn(true);
    assertDoesNotThrow(
        () ->
            iamService.verifyResourceTypeAdminAuthorizedWithLogging(
                TEST_USER,
                IamResourceType.DATASNAPSHOT,
                IamAction.ADMIN_READ_SUMMARY_INFORMATION,
                id));
  }

  @Test
  void testVerifyResourceTypeAdminAuthorizedWithLoggingFalse() throws InterruptedException {
    UUID id = UUID.randomUUID();
    when(iamProvider.getResourceTypeAdminPermission(
            TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION))
        .thenReturn(false);
    assertThrows(
        IamForbiddenException.class,
        () ->
            iamService.verifyResourceTypeAdminAuthorizedWithLogging(
                TEST_USER,
                IamResourceType.DATASNAPSHOT,
                IamAction.ADMIN_READ_SUMMARY_INFORMATION,
                id));
  }

  @Test
  void testVerifyResourceTypeAdminAuthorizedTrue() throws InterruptedException {
    when(iamProvider.getResourceTypeAdminPermission(
            TEST_USER, IamResourceType.DATAREPO, IamAction.LIST_JOBS))
        .thenReturn(true);
    assertDoesNotThrow(
        () ->
            iamService.verifyResourceTypeAdminAuthorized(
                TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION));
  }

  @Test
  void testVerifyResourceTypeAdminAuthorizedFalse() throws InterruptedException {
    when(iamProvider.getResourceTypeAdminPermission(
            TEST_USER, IamResourceType.DATAREPO, IamAction.LIST_JOBS))
        .thenReturn(false);
    assertThrows(
        IamForbiddenException.class,
        () ->
            iamService.verifyResourceTypeAdminAuthorized(
                TEST_USER, IamResourceType.DATAREPO, IamAction.LIST_JOBS));
  }

  @Test
  void testGetGroup() throws InterruptedException {
    String groupName = "groupName";
    String groupEmail = "groupEmail";
    String accessToken = "accessToken";
    when(googleCredentialsService.getApplicationDefaultAccessToken(any())).thenReturn(accessToken);
    when(iamProvider.getGroup(accessToken, groupName)).thenReturn(groupEmail);
    assertEquals(groupEmail, iamService.getGroup(groupName));
  }

  @Test
  void testGetGroupPolicyEmails() throws InterruptedException {
    String groupName = "groupName";
    String policyName = IamRole.MEMBER.toString();
    String accessToken = "accessToken";
    when(googleCredentialsService.getApplicationDefaultAccessToken(any())).thenReturn(accessToken);
    when(iamProvider.getGroupPolicyEmails(accessToken, groupName, policyName))
        .thenReturn(List.of());
    assertEquals(iamService.getGroupPolicyEmails(groupName, policyName), List.of());
  }

  @Test
  void testAddEmailToGroup() throws InterruptedException {
    String groupName = "groupName";
    String policyName = IamRole.MEMBER.toString();
    String email = "user@gmail.com";
    String accessToken = "accessToken";
    when(googleCredentialsService.getApplicationDefaultAccessToken(any())).thenReturn(accessToken);
    when(iamProvider.addGroupPolicyEmail(accessToken, groupName, policyName, email))
        .thenReturn(List.of(email));
    assertEquals(iamService.addEmailToGroup(groupName, policyName, email), List.of(email));
  }

  @Test
  void testRemoveEmailFromGroup() throws InterruptedException {
    String groupName = "groupName";
    String policyName = IamRole.MEMBER.toString();
    String email = "user@gmail.com";
    String accessToken = "accessToken";
    when(googleCredentialsService.getApplicationDefaultAccessToken(any())).thenReturn(accessToken);
    when(iamProvider.removeGroupPolicyEmail(accessToken, groupName, policyName, email))
        .thenReturn(List.of());
    assertEquals(iamService.removeEmailFromGroup(groupName, policyName, email), List.of());
  }

  @Test
  void setResourceParent() throws InterruptedException {
    UUID childId = UUID.randomUUID();
    UUID parentId = UUID.randomUUID();
    iamService.setResourceParent(
        TEST_USER.getToken(),
        IamResourceType.DATASNAPSHOT,
        childId,
        IamResourceType.DATASET,
        parentId);
    verify(iamProvider)
        .setResourceParent(
            TEST_USER.getToken(),
            IamResourceType.DATASNAPSHOT,
            childId,
            IamResourceType.DATASET,
            parentId);
  }

  @Test
  void deleteResourceParent() throws InterruptedException {
    UUID childId = UUID.randomUUID();
    iamService.deleteResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, childId);
    verify(iamProvider)
        .deleteResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, childId);
  }

  @Test
  void getResourceParent() throws InterruptedException {
    UUID childId = UUID.randomUUID();
    FullyQualifiedResourceId parent = new FullyQualifiedResourceId();
    when(iamProvider.getResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, childId))
        .thenReturn(parent);
    assertEquals(
        parent,
        iamService.getResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, childId));
  }

  @Test
  void listResourceChildren() throws InterruptedException {
    UUID parentId = UUID.randomUUID();
    List<FullyQualifiedResourceId> children = List.of(new FullyQualifiedResourceId());
    when(iamProvider.listResourceChildren(TEST_USER.getToken(), IamResourceType.DATASET, parentId))
        .thenReturn(children);
    assertEquals(
        children,
        iamService.listResourceChildren(TEST_USER.getToken(), IamResourceType.DATASET, parentId));
  }

  @Test
  void getPolicyPublicV2() throws InterruptedException {
    UUID resourceId = UUID.randomUUID();
    when(iamProvider.getPolicyPublicV2(
            TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, resourceId, IamRole.READER.name()))
        .thenReturn(true);
    assertEquals(
        true,
        iamService.getPolicyPublicV2(
            TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, resourceId, IamRole.READER.name()));
  }

  @Test
  void setPolicyPublicV2() throws InterruptedException {
    UUID resourceId = UUID.randomUUID();
    iamService.setPolicyPublicV2(
        TEST_USER.getToken(),
        IamResourceType.DATASNAPSHOT,
        resourceId,
        IamRole.READER.name(),
        true);
    verify(iamProvider)
        .setPolicyPublicV2(
            TEST_USER.getToken(),
            IamResourceType.DATASNAPSHOT,
            resourceId,
            IamRole.READER.name(),
            true);
  }
}
