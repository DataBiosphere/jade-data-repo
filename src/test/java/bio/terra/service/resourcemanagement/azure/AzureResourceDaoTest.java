package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.ProfileDao;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class AzureResourceDaoTest {

  @Autowired private ProfileDao profileDao;

  @Autowired private AzureResourceDao azureResourceDao;

  private BillingProfileModel billingProfile;
  private List<AzureApplicationDeploymentResource> applicationDeployments;
  private List<AzureStorageAccountResource> storageAccounts;

  @Before
  public void setup() throws IOException, InterruptedException {
    UUID datasetId = UUID.randomUUID();
    UUID snapshotId = UUID.randomUUID();

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
        azureResourceDao.createAndLockStorage(
            ProfileFixtures.randomizeName("sa1"),
            datasetId.toString(),
            appDeployment,
            AzureRegion.DEFAULT_AZURE_REGION,
            null);
    storageAccounts.add(sa1);

    var sa2 =
        azureResourceDao.createAndLockStorage(
            ProfileFixtures.randomizeName("sa2"),
            snapshotId.toString(),
            appDeployment,
            AzureRegion.DEFAULT_AZURE_REGION,
            null);
    storageAccounts.add(sa2);
  }

  @After
  public void teardown() {
    boolean allStorageDeleted =
        storageAccounts.stream()
            .allMatch(
                sa ->
                    azureResourceDao.markForDeleteStorageAccountMetadata(
                        sa.getName(), sa.getTopLevelContainer(), null));

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
              "Can fetch storage account by name and container",
              azureResourceDao.getStorageAccount(
                  sa.getName(),
                  sa.getTopLevelContainer(),
                  sa.getApplicationResource().getAzureApplicationDeploymentName()),
              equalTo(sa));
        });
  }

  @Test
  public void markForDeleteStorageAccountMetadata() {
    var appDeploymentId = applicationDeployments.get(0).getId();
    assertThat(
        "Before marking for delete, confirm that 2 storage accounts are returned when marked_for_delete = false",
        azureResourceDao
            .retrieveStorageAccountsByApplicationResource(appDeploymentId, false)
            .size(),
        equalTo(2));

    // Mark one storage account for delete
    var storageAccount1 = storageAccounts.get(0);
    assertThat(
        "Can mark storage account for delete",
        azureResourceDao.markForDeleteStorageAccountMetadata(
            storageAccount1.getName(), storageAccount1.getTopLevelContainer(), null),
        equalTo(true));

    var storageAccountsMarkedForDelete =
        azureResourceDao.retrieveStorageAccountsByApplicationResource(appDeploymentId, true);
    assertThat(
        "Only 1 storage accounts is returned", storageAccountsMarkedForDelete.size(), equalTo(1));
    assertThat(
        "Storage account marked for delete is returned",
        storageAccountsMarkedForDelete.get(0).getStorageAccountId(),
        equalTo(storageAccount1.getStorageAccountId()));
  }
}
