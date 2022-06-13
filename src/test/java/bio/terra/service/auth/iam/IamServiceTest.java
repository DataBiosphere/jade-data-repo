package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
public class IamServiceTest {
  private static final UUID ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

  @Mock private IamProviderInterface iamProvider;

  @Mock private ConfigurationService configurationService;

  private IamService iamService;
  private AuthenticatedUserRequest authenticatedUserRequest;

  @Before
  public void setup() {
    when(configurationService.getParameterValue(ConfigEnum.AUTH_CACHE_SIZE)).thenReturn(1);
    iamService = new IamService(iamProvider, configurationService);
    authenticatedUserRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DatasetUnit")
            .setEmail("dataset@unit.com")
            .setToken("token")
            .build();
  }

  @Test
  public void testAddPolicyMember() throws InterruptedException {
    var policyModel = new PolicyModel();
    String policyName = "policyName";
    String email = "email";
    when(iamProvider.addPolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email))
        .thenReturn(policyModel);

    PolicyModel result =
        iamService.addPolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    verify(iamProvider, times(1))
        .addPolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    assertEquals(policyModel, result);
  }

  @Test
  public void testDeletePolicyMember() throws InterruptedException {
    var policyModel = new PolicyModel();
    String policyName = "policyName";
    String email = "email";
    when(iamProvider.deletePolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email))
        .thenReturn(policyModel);

    PolicyModel result =
        iamService.deletePolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    verify(iamProvider, times(1))
        .deletePolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    assertEquals(policyModel, result);
  }

  @Test
  public void testVerifyAuthorizations() throws Exception {
    IamResourceType resourceType = IamResourceType.DATASET;
    String id = ID.toString();

    Set<IamAction> hasActions = EnumSet.of(IamAction.MANAGE_SCHEMA, IamAction.READ_DATA);
    when(iamProvider.listActions(authenticatedUserRequest, resourceType, id))
        .thenReturn(hasActions.stream().map(IamAction::toString).collect(Collectors.toList()));

    // Checking authorizations for actions associated with the caller should not throw.
    iamService.verifyAuthorizations(authenticatedUserRequest, resourceType, id, Set.of());
    iamService.verifyAuthorizations(authenticatedUserRequest, resourceType, id, hasActions);

    Set<IamAction> missingActions = EnumSet.of(IamAction.UPDATE_PASSPORT_IDENTIFIER);
    Set<IamAction> requiredActions = EnumSet.copyOf(hasActions);
    requiredActions.addAll(missingActions);

    IamForbiddenException thrown =
        assertThrows(
            IamForbiddenException.class,
            () ->
                iamService.verifyAuthorizations(
                    authenticatedUserRequest, resourceType, id, requiredActions),
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
}
