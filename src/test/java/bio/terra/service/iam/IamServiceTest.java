package bio.terra.service.iam;

import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import bio.terra.model.PolicyModel;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Category(Unit.class)
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class IamServiceTest {
    @Mock
    private IamProviderInterface iamProvider;

    @Mock
    private ConfigurationService configurationService;

    private IamService iamService;

    @Before
    public void setup() {
        when(configurationService.getParameterValue(eq(ConfigEnum.AUTH_CACHE_SIZE))).thenReturn(1);
        iamService = new IamService(iamProvider, configurationService);
    }


    @Test
    public void testAddPolicyMember() throws InterruptedException{
        var policyModel = new PolicyModel();
        when(iamProvider.addPolicyMember(any(), any(), any(), any(), any())).thenReturn(policyModel);
        PolicyModel result = iamService.addPolicyMember(
                new AuthenticatedUserRequest(),
                IamResourceType.SPEND_PROFILE,
                UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "policyName",
                "email"
        );
        assertEquals(policyModel, result);
    }

    @Test
    public void testDeletePolicyMember() throws InterruptedException{
        var policyModel = new PolicyModel();
        when(iamProvider.deletePolicyMember(any(), any(), any(), any(), any())).thenReturn(policyModel);
        PolicyModel result = iamService.deletePolicyMember(
                new AuthenticatedUserRequest(),
                IamResourceType.SPEND_PROFILE,
                UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "policyName",
                "email"
        );
        assertEquals(policyModel, result);
    }
}
