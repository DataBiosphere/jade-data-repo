package bio.terra.service.resourcemanagement.azure;

import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.DEFAULT_REGION_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.MANAGED_RESOURCE_GROUP_ID_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.PARAMETERS_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.PARAMETER_VALUE_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.STORAGE_PREFIX_KEY;
import static bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService.STORAGE_TYPE_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.AzureStorageAccountSkuType;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.exception.AzureResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.MismatchedBillingProfilesException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import com.azure.resourcemanager.resources.models.GenericResources;
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
class AzureApplicationDeploymentServiceTest {

  @Mock private AzureResourceDao resourceDao;
  @Mock private GenericResource genericResource;
  @Mock private GenericResources genericResources;
  @Mock private AzureResourceManager client;
  @Mock private AzureResourceConfiguration resourceConfiguration;

  private AzureApplicationDeploymentService service;

  @BeforeEach
  void setUp() {
    service =
        new AzureApplicationDeploymentService(
            resourceDao, resourceConfiguration, new ApplicationConfiguration().objectMapper());
  }

  @Test
  void getOrRegisterApplicationDeployment_register() {
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
    UUID registrationId = UUID.randomUUID();
    when(resourceDao.createApplicationDeployment(any())).thenReturn(registrationId);

    AzureApplicationDeploymentResource appResource =
        service.getOrRegisterApplicationDeployment(billingProfileModel);

    assertAll(
        "Verify application deployment properties",
        () ->
            assertThat(
                "Application name matches",
                appResource.getAzureApplicationDeploymentName(),
                equalTo(billingProfileModel.getApplicationDeploymentName())),
        () ->
            assertThat(
                "Managed resource group matches",
                appResource.getAzureResourceGroupName(),
                equalTo("mgd-grp-1")),
        () ->
            assertThat(
                "Default region matches",
                appResource.getDefaultRegion(),
                equalTo(AzureRegion.AUSTRALIA)),
        () -> assertThat("Prefix matches", appResource.getStorageAccountPrefix(), equalTo("tdr")),
        () ->
            assertThat(
                "Storage account type matches",
                appResource.getStorageAccountSkuType(),
                equalTo(AzureStorageAccountSkuType.STANDARD_LRS)),
        () -> assertThat("Registration ID matches", appResource.getId(), equalTo(registrationId)));
  }

  @Test
  void getOrRegisterApplicationDeployment_get() {
    BillingProfileModel billingProfileModel = ProfileFixtures.randomAzureBillingProfile();
    AzureApplicationDeploymentResource existingAppResource =
        new AzureApplicationDeploymentResource()
            .profileId(billingProfileModel.getId())
            .azureApplicationDeploymentName("existing-app-resource");
    when(resourceDao.retrieveApplicationDeploymentByName(
            billingProfileModel.getApplicationDeploymentName()))
        .thenReturn(existingAppResource);

    assertThat(
        "Existing application resource for profile is returned",
        service.getOrRegisterApplicationDeployment(billingProfileModel),
        equalTo(existingAppResource));
    verify(resourceDao, never()).createApplicationDeployment(any());
  }

  @Test
  void getOrRegisterApplicationDeployment_throwsOnMismatchedBillingProfile() {
    BillingProfileModel billingProfileModel = ProfileFixtures.randomAzureBillingProfile();
    UUID differentProfileId = UUID.randomUUID();
    AzureApplicationDeploymentResource existingAppResource =
        new AzureApplicationDeploymentResource()
            .profileId(differentProfileId)
            .azureApplicationDeploymentName("existing-app-resource");
    when(resourceDao.retrieveApplicationDeploymentByName(
            billingProfileModel.getApplicationDeploymentName()))
        .thenReturn(existingAppResource);

    assertThrows(
        MismatchedBillingProfilesException.class,
        () -> service.getOrRegisterApplicationDeployment(billingProfileModel),
        "Throws when an application deployment already exists for a different profile");
    verify(resourceDao, never()).createApplicationDeployment(any());
  }
}
