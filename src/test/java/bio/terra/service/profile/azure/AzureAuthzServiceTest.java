package bio.terra.service.profile.azure;


import bio.terra.common.category.Unit;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import com.azure.resourcemanager.resources.models.GenericResources;
import com.google.cloud.resourcemanager.ResourceManagerException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.UUID;

import static bio.terra.service.profile.azure.AzureAuthzService.AUTH_PARAM_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@Category(Unit.class)
public class AzureAuthzServiceTest {

    private static final String USER_EMAIL = "";
    private static final UUID SUBSCRIPTION_ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    private static final String RESOURCE_GROUP_NAME = "tdr_rg";
    private static final String APPLICATION_DEPLOYMENT_NAME = "myapp";
    private static final String RESOURCE_ID = "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/resourceGroups" +
        "/tdr_rg/providers/Microsoft.Solutions/applications/myapp";

    @Mock
    private GenericResource genericResource;
    @Mock
    private GenericResources genericResources;
    @Mock
    private AzureResourceManager resourceManager;
    @Mock
    private AzureResourceConfiguration resourceConfiguration;

    private AzureAuthzService azureAuthzService;

    @Before
    public void setUp() throws Exception {
        when(genericResource.properties()).thenReturn(Map.of(
            "parameters", Map.of(AUTH_PARAM_KEY, Map.of(
                "value", USER_EMAIL))));
        when(resourceManager.genericResources()).thenReturn(genericResources);
        when(resourceConfiguration.getClient(SUBSCRIPTION_ID)).thenReturn(resourceManager);
        when(genericResources.getById(RESOURCE_ID)).thenReturn(genericResource);
        azureAuthzService = new AzureAuthzService(resourceConfiguration);
    }

    @Test
    public void testValidateHappyPath() {
        assertThat(azureAuthzService.canAccess(
            new AuthenticatedUserRequest().email(USER_EMAIL),
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            APPLICATION_DEPLOYMENT_NAME), equalTo(true));
    }

    @Test
    public void testValidateUserDoesNotHaveAccess() {
        assertThat(azureAuthzService.canAccess(
            new AuthenticatedUserRequest().email("voldemort.admin@test.firecloud.org"),
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            APPLICATION_DEPLOYMENT_NAME), equalTo(false));
    }

    @Test
    public void testValidateResourceNotFound() {
        when(genericResources.getById(RESOURCE_ID))
            .thenThrow(ResourceManagerException.class);

        assertThat(azureAuthzService.canAccess(
            new AuthenticatedUserRequest().email(USER_EMAIL),
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            APPLICATION_DEPLOYMENT_NAME), equalTo(false));
    }
}
