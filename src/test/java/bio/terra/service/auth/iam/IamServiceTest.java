package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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
public class IamServiceTest {
  private static final UUID ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

  @Mock private IamProviderInterface iamProvider;

  @Mock private ConfigurationService configurationService;

  private IamService iamService;
  private AuthenticatedUserRequest authenticatedUserRequest;

  @Before
  public void setup() {
    when(configurationService.getParameterValue(eq(ConfigEnum.AUTH_CACHE_SIZE))).thenReturn(1);
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
            eq(authenticatedUserRequest),
            eq(IamResourceType.SPEND_PROFILE),
            eq(ID),
            eq(policyName),
            eq(email)))
        .thenReturn(policyModel);

    PolicyModel result =
        iamService.addPolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    verify(iamProvider, times(1))
        .addPolicyMember(
            eq(authenticatedUserRequest),
            eq(IamResourceType.SPEND_PROFILE),
            eq(ID),
            eq(policyName),
            eq(email));
    assertEquals(policyModel, result);
  }

  @Test
  public void testDeletePolicyMember() throws InterruptedException {
    var policyModel = new PolicyModel();
    String policyName = "policyName";
    String email = "email";
    when(iamProvider.deletePolicyMember(
            eq(authenticatedUserRequest),
            eq(IamResourceType.SPEND_PROFILE),
            eq(ID),
            eq(policyName),
            eq(email)))
        .thenReturn(policyModel);

    PolicyModel result =
        iamService.deletePolicyMember(
            authenticatedUserRequest, IamResourceType.SPEND_PROFILE, ID, policyName, email);
    verify(iamProvider, times(1))
        .deletePolicyMember(
            eq(authenticatedUserRequest),
            eq(IamResourceType.SPEND_PROFILE),
            eq(ID),
            eq(policyName),
            eq(email));
    assertEquals(policyModel, result);
  }

  @Test
  public void testVerifyAuthorizations() throws Exception {
    IamResourceType resourceType = IamResourceType.DATASET;
    String id = ID.toString();

    Set<IamAction> hasActions = EnumSet.of(IamAction.MANAGE_SCHEMA, IamAction.READ_DATA);
    when(iamProvider.listActions(eq(authenticatedUserRequest), eq(resourceType), eq(id)))
        .thenReturn(hasActions);

    // Checking authorizations for actions associated with the caller should not throw.
    iamService.verifyAuthorizations(authenticatedUserRequest, resourceType, id, List.of());
    iamService.verifyAuthorizations(authenticatedUserRequest, resourceType, id, hasActions);

    List<IamAction> missingActions = List.of(IamAction.UPDATE_PASSPORT_IDENTIFIER);
    List<IamAction> requiredActions = new ArrayList<>();
    requiredActions.addAll(hasActions);
    requiredActions.addAll(missingActions);

    Throwable thrown =
        assertThrows(
            IamForbiddenException.class,
            () ->
                iamService.verifyAuthorizations(
                    authenticatedUserRequest, resourceType, id, requiredActions),
            "Authorization verification throws if the caller is missing a required action");
    assertThat(
        "Error message contains missing actions",
        thrown.getMessage(),
        containsString(missingActions.toString()));
  }
}
