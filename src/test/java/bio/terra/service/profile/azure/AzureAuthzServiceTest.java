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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureAuthzServiceTest {

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

  @BeforeEach
  void setUp() {
    when(resourceManager.genericResources()).thenReturn(genericResources);
    when(resourceConfiguration.getClient(SUBSCRIPTION_ID)).thenReturn(resourceManager);
    when(genericResources.getById(RESOURCE_ID, resourceConfiguration.apiVersion()))
        .thenReturn(genericResource);
    azureAuthzService = new AzureAuthzService(resourceConfiguration);
  }

  private void mockGenericResourceProperties() {
    when(genericResource.properties())
        .thenReturn(Map.of("parameters", Map.of(AUTH_PARAM_KEY, Map.of("value", USER_EMAIL))));
  }

  @Test
  void testValidateHappyPath() {
    mockGenericResourceProperties();
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
  void testValidateUserDoesNotHaveAccess() {
    mockGenericResourceProperties();
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
  void testValidateResourceNotFound() {
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
