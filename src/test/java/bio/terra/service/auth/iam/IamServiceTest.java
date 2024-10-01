package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
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
    var policyModel = new PolicyModel();
    String policyName = "policyName";
    String email = "email";
    when(iamProvider.addPolicyMember(
            TEST_USER, IamResourceType.SPEND_PROFILE, ID, policyName, email))
        .thenReturn(policyModel);

    PolicyModel result =
        iamService.addPolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    verify(iamProvider)
        .addPolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    assertEquals(policyModel, result);
  }

  @Test
  void testDeletePolicyMember() throws InterruptedException {
    var policyModel = new PolicyModel();
    String policyName = "policyName";
    String email = "email";
    when(iamProvider.deletePolicyMember(
            TEST_USER, IamResourceType.SPEND_PROFILE, ID, policyName, email))
        .thenReturn(policyModel);

    PolicyModel result =
        iamService.deletePolicyMember(
            TEST_USER, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    verify(iamProvider)
        .deletePolicyMember(TEST_USER, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    assertEquals(policyModel, result);
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
  void testVerifyResourceTypeAdminAuthorizedTrue() throws InterruptedException {
    when(iamProvider.getResourceTypeAdminPermission(
            TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION))
        .thenReturn(true);
    assertDoesNotThrow(
        () ->
            iamService.verifyResourceTypeAdminAuthorized(
                TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION));
  }

  @Test
  void testVerifyResourceTypeAdminAuthorizedFalse() throws InterruptedException {
    when(iamProvider.getResourceTypeAdminPermission(
            TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION))
        .thenReturn(false);
    assertThrows(
        IamForbiddenException.class,
        () ->
            iamService.verifyResourceTypeAdminAuthorized(
                TEST_USER, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION));
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
        .thenReturn(new ArrayList<>());
    assertEquals(iamService.getGroupPolicyEmails(groupName, policyName), new ArrayList<>());
  }

  @Test
  void testAddEmailToGroup() throws InterruptedException {
    String groupName = "groupName";
    String policyName = IamRole.MEMBER.toString();
    String email = "user@gmail.com";
    String accessToken = "accessToken";
    when(googleCredentialsService.getApplicationDefaultAccessToken(any())).thenReturn(accessToken);
    when(iamProvider.addGroupPolicyEmail(accessToken, groupName, policyName, email))
        .thenReturn(new ArrayList<>(List.of("user@gmail.com")));
    assertEquals(
        iamService.addEmailToGroup(groupName, policyName, email),
        new ArrayList<>(List.of("user@gmail.com")));
  }

  @Test
  void testRemoveEmailFromGroup() throws InterruptedException {
    String groupName = "groupName";
    String policyName = IamRole.MEMBER.toString();
    String email = "user@gmail.com";
    String accessToken = "accessToken";
    when(googleCredentialsService.getApplicationDefaultAccessToken(any())).thenReturn(accessToken);
    when(iamProvider.removeGroupPolicyEmail(accessToken, groupName, policyName, email))
        .thenReturn(new ArrayList<>());
    assertEquals(iamService.removeEmailFromGroup(groupName, policyName, email), new ArrayList<>());
  }
}
