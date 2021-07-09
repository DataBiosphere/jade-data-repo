package bio.terra.datarepo.service.resourcemanagement.azure;

import static bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentService.DEFAULT_REGION_KEY;
import static bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentService.MANAGED_RESOURCE_GROUP_ID_KEY;
import static bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentService.PARAMETERS_KEY;
import static bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentService.PARAMETER_VALUE_KEY;
import static bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentService.STORAGE_PREFIX_KEY;
import static bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentService.STORAGE_TYPE_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.app.configuration.ApplicationConfiguration;
import bio.terra.datarepo.app.model.AzureRegion;
import bio.terra.datarepo.app.model.AzureStorageAccountSkuType;
import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.common.fixtures.ProfileFixtures;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.service.resourcemanagement.exception.AzureResourceNotFoundException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import com.azure.resourcemanager.resources.models.GenericResources;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class AzureApplicationDeploymentServiceTest {

  @Mock private AzureResourceDao resourceDao;
  @Mock private GenericResource genericResource;
  @Mock private GenericResources genericResources;
  @Mock private AzureResourceManager client;
  @Mock private AzureResourceConfiguration resourceConfiguration;

  private AzureApplicationDeploymentService service;

  @Before
  public void setUp() throws Exception {
    service =
        new AzureApplicationDeploymentService(
            resourceDao, resourceConfiguration, new ApplicationConfiguration().objectMapper());
  }

  @Test
  public void testGetOrRegisterApplicationDeployment() {
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
    when(genericResources.getById(
            "/subscriptions/"
                + billingProfileModel.getSubscriptionId()
                + "/resourceGroups"
                + "/"
                + billingProfileModel.getResourceGroupName()
                + "/providers/Microsoft.Solutions/applications/"
                + billingProfileModel.getApplicationDeploymentName()))
        .thenReturn(genericResource);
    when(client.genericResources()).thenReturn(genericResources);
    when(resourceDao.retrieveApplicationDeploymentByName(
            billingProfileModel.getApplicationDeploymentName()))
        .thenThrow(AzureResourceNotFoundException.class);
    when(resourceConfiguration.getClient(UUID.fromString(billingProfileModel.getSubscriptionId())))
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
