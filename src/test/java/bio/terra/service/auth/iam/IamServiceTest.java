package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    when(configurationService.getParameterValue(ConfigEnum.AUTH_CACHE_TIMEOUT_SECONDS))
        .thenReturn(0);
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
  public void testVerifyAuthorization() throws Exception {
    IamResourceType resourceType = IamResourceType.DATASET;
    String id = ID.toString();
    IamAction action = IamAction.READ_DATA;

    when(iamProvider.isAuthorized(authenticatedUserRequest, resourceType, id, action))
        .thenReturn(true);
    // Checking authorization for an action associated with the caller should not throw.
    iamService.verifyAuthorization(authenticatedUserRequest, resourceType, id, action);

    when(iamProvider.isAuthorized(authenticatedUserRequest, resourceType, id, action))
        .thenReturn(false);
    IamForbiddenException thrown =
        assertThrows(
            IamForbiddenException.class,
            () ->
                iamService.verifyAuthorization(authenticatedUserRequest, resourceType, id, action),
            "Authorization verification throws if the caller is missing the action");
    assertThat(
        "Error message reflects cause",
        thrown.getMessage(),
        containsString("does not have required action: " + action));
  }
}
