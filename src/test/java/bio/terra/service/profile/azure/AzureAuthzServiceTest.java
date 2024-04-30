package bio.terra.service.profile.azure;

import static bio.terra.service.profile.azure.AzureAuthzService.AUTH_PARAM_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import com.azure.resourcemanager.resources.models.GenericResources;
import com.google.cloud.resourcemanager.ResourceManagerException;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category(Unit.class)
public class AzureAuthzServiceTest {

  private static final String USER_EMAIL = "";
  private static final UUID SUBSCRIPTION_ID =
      UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
  private static final String RESOURCE_GROUP_NAME = "tdr_rg";
  private static final String APPLICATION_DEPLOYMENT_NAME = "myapp";
  private static final String RESOURCE_ID =
      "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/resourceGroups"
          + "/tdr_rg/providers/Microsoft.Solutions/applications/myapp";

  @Mock private GenericResource genericResource;
  @Mock private GenericResources genericResources;
  @Mock private AzureResourceManager resourceManager;
  @Mock private AzureResourceConfiguration resourceConfiguration;

  private AzureAuthzService azureAuthzService;

  @Before
  public void setUp() throws Exception {
    when(genericResource.properties())
        .thenReturn(Map.of("parameters", Map.of(AUTH_PARAM_KEY, Map.of("value", USER_EMAIL))));
    when(resourceManager.genericResources()).thenReturn(genericResources);
    when(resourceConfiguration.getClient(SUBSCRIPTION_ID)).thenReturn(resourceManager);
    when(genericResources.getById(RESOURCE_ID, resourceConfiguration.apiVersion()))
        .thenReturn(genericResource);
    azureAuthzService = new AzureAuthzService(resourceConfiguration);
  }

  @Test
  public void testValidateHappyPath() {
    assertThat(
        azureAuthzService.canAccess(
            AuthenticatedUserRequest.builder()
                .setSubjectId("DatasetUnit")
                .setEmail(USER_EMAIL)
                .setToken("token")
                .build(),
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            APPLICATION_DEPLOYMENT_NAME),
        equalTo(true));
  }

  @Test
  public void testValidateUserDoesNotHaveAccess() {
    assertThat(
        azureAuthzService.canAccess(
            AuthenticatedUserRequest.builder()
                .setSubjectId("DatasetUnit")
                .setEmail("voldemort.admin@test.firecloud.org")
                .setToken("token")
                .build(),
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            APPLICATION_DEPLOYMENT_NAME),
        equalTo(false));
  }

  @Test
  public void testValidateResourceNotFound() {
    when(genericResources.getById(RESOURCE_ID, resourceConfiguration.apiVersion()))
        .thenThrow(ResourceManagerException.class);

    assertThat(
        azureAuthzService.canAccess(
            AuthenticatedUserRequest.builder()
                .setSubjectId("DatasetUnit")
                .setEmail(USER_EMAIL)
                .setToken("token")
                .build(),
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            APPLICATION_DEPLOYMENT_NAME),
        equalTo(false));
  }
}
