package bio.terra.service.iam;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.PolicyModel;
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

  @Before
  public void setup() {
    when(configurationService.getParameterValue(eq(ConfigEnum.AUTH_CACHE_SIZE))).thenReturn(1);
    iamService = new IamService(iamProvider, configurationService);
  }

  @Test
  public void testAddPolicyMember() throws InterruptedException {
    var authenticatedUserRequest = new AuthenticatedUserRequest();
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
    var authenticatedUserRequest = new AuthenticatedUserRequest();
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
}
