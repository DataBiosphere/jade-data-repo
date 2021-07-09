package bio.terra.datarepo.service.resourcemanagement.azure;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import bio.terra.datarepo.app.model.AzureRegion;
import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.common.fixtures.ProfileFixtures;
import bio.terra.datarepo.common.fixtures.ResourceFixtures;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.service.profile.ProfileDao;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class AzureResourceDaoTest {

  @Autowired private ProfileDao profileDao;

  @Autowired private AzureResourceDao azureResourceDao;

  private BillingProfileModel billingProfile;
  private List<AzureApplicationDeploymentResource> applicationDeployments;
  private List<AzureStorageAccountResource> storageAccounts;

  @Before
  public void setup() throws IOException, InterruptedException {
    // Initialize list;
    applicationDeployments = new ArrayList<>();
    storageAccounts = new ArrayList<>();

    // One billing profile
    var profileRequest = ProfileFixtures.randomizeAzureBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    // one azure application deployment resources and two storage accounts
    var appDeployment = ResourceFixtures.randomApplicationDeploymentResource(billingProfile);
    var appId = azureResourceDao.createApplicationDeployment(appDeployment);
    appDeployment.id(appId);
    applicationDeployments.add(appDeployment);

    var sa1 =
        azureResourceDao.createAndLockStorageAccount(
            ProfileFixtures.randomizeName("sa1"),
            appDeployment,
            AzureRegion.DEFAULT_AZURE_REGION,
            null);
    storageAccounts.add(sa1);

    var sa2 =
        azureResourceDao.createAndLockStorageAccount(
            ProfileFixtures.randomizeName("sa2"),
            appDeployment,
            AzureRegion.DEFAULT_AZURE_REGION,
            null);
    storageAccounts.add(sa2);
  }

  @After
  public void teardown() {
    boolean allStorageDeleted =
        storageAccounts.stream()
            .allMatch(sa -> azureResourceDao.deleteStorageAccountMetadata(sa.getName(), null));

    azureResourceDao.markUnusedApplicationDeploymentsForDelete(billingProfile.getId());
    azureResourceDao.deleteApplicationDeploymentMetadata(
        applicationDeployments.stream()
            .map(AzureApplicationDeploymentResource::getId)
            .collect(Collectors.toList()));

    profileDao.deleteBillingProfileById(billingProfile.getId());

    assertThat("All storage accounts were deleted", allStorageDeleted, equalTo(true));
  }

  @Test
  public void oneApplicationOneBillingProfilesTwoStorageAccounts() throws Exception {
    var retrievedAppDeployments =
        azureResourceDao.retrieveApplicationDeploymentsByBillingProfileId(billingProfile.getId());

    assertThat(
        "Application Deployment count should be 1", retrievedAppDeployments.size(), equalTo(1));

    var retrievedAppDeployment = retrievedAppDeployments.get(0);

    // Assert that application deployment can be fetched
    assertThat(
        "Can fetch application deployment by id",
        azureResourceDao.retrieveApplicationDeploymentById(retrievedAppDeployment.getId()).getId(),
        equalTo(retrievedAppDeployment.getId()));
    assertThat(
        "Can fetch application deployment by name",
        azureResourceDao
            .retrieveApplicationDeploymentByName(
                retrievedAppDeployment.getAzureApplicationDeploymentName())
            .getAzureApplicationDeploymentName(),
        equalTo(retrievedAppDeployment.getAzureApplicationDeploymentName()));

    // Assert that storage accounts can be fetched
    storageAccounts.forEach(
        sa -> {
          assertThat(
              "Can fetch storage account by id",
              azureResourceDao.retrieveStorageAccountById(sa.getResourceId()),
              equalTo(sa));
          assertThat(
              "Can fetch storage account by name",
              azureResourceDao.getStorageAccount(
                  sa.getName(), sa.getApplicationResource().getAzureApplicationDeploymentName()),
              equalTo(sa));
        });
  }
}
