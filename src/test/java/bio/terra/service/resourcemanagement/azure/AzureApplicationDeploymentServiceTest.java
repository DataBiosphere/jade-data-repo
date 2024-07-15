package bio.terra.service.resourcemanagement.azure;

import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.DEFAULT_REGION_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.MANAGED_RESOURCE_GROUP_ID_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.PARAMETERS_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.PARAMETER_VALUE_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.STORAGE_PREFIX_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.STORAGE_TYPE_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.AzureStorageAccountSkuType;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.exception.AzureResourceNotFoundException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import com.azure.resourcemanager.resources.models.GenericResources;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureApplicationDeploymentServiceTest {

  @Mock private AzureResourceDao resourceDao;
  @Mock private GenericResource genericResource;
  @Mock private GenericResources genericResources;
  @Mock private AzureResourceManager client;
  @Mock private AzureResourceConfiguration resourceConfiguration;

  private AzureApplicationDeploymentService service;

  @BeforeEach
  void setUp() throws Exception {
    service =
        new AzureApplicationDeploymentService(
            resourceDao, resourceConfiguration, new ApplicationConfiguration().objectMapper());
  }

  @Test
  void testGetOrRegisterApplicationDeployment() {
    BillingProfileModel billingProfileModel = ProfileFixtures.randomAzureBillingProfile();
    when(genericResource.properties())
        .thenReturn(
            Map.of(
                MANAGED_RESOURCE_GROUP_ID_KEY,
                "mgd-grp-1",
                PARAMETERS_KEY,
                Map.of(
                    DEFAULT_REGION_KEY, Map.of(PARAMETER_VALUE_KEY, "AUSTRALIA"),
                    STORAGE_PREFIX_KEY, Map.of(PARAMETER_VALUE_KEY, "tdr"),
                    STORAGE_TYPE_KEY, Map.of(PARAMETER_VALUE_KEY, "Standard_LRS"))));
    String appResourceId = MetadataDataAccessUtils.getApplicationDeploymentId(billingProfileModel);
    when(genericResources.getById(appResourceId, resourceConfiguration.apiVersion()))
        .thenReturn(genericResource);
    when(client.genericResources()).thenReturn(genericResources);
    when(resourceDao.retrieveApplicationDeploymentByName(
            billingProfileModel.getApplicationDeploymentName()))
        .thenThrow(AzureResourceNotFoundException.class);
    when(resourceConfiguration.getClient(billingProfileModel.getSubscriptionId()))
        .thenReturn(client);

    AzureApplicationDeploymentResource appResource =
        service.getOrRegisterApplicationDeployment(billingProfileModel);

    assertThat(
        "Application name matches",
        appResource.getAzureApplicationDeploymentName(),
        equalTo(billingProfileModel.getApplicationDeploymentName()));
    assertThat(
        "Managed resource group matches",
        appResource.getAzureResourceGroupName(),
        equalTo("mgd-grp-1"));
    assertThat(
        "Default region matches", appResource.getDefaultRegion(), equalTo(AzureRegion.AUSTRALIA));
    assertThat("Prefix matches", appResource.getStorageAccountPrefix(), equalTo("tdr"));
    assertThat(
        "Storage account type matches",
        appResource.getStorageAccountSkuType(),
        equalTo(AzureStorageAccountSkuType.STANDARD_LRS));
  }
}
