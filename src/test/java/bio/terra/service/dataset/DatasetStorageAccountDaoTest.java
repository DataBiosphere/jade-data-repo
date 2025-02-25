package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.stairway.ShortUUID;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class DatasetStorageAccountDaoTest {

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private DatasetStorageAccountDao datasetStorageAccountDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private AzureResourceDao azureResourceDao;

  private UUID applicationId;
  private UUID projectId;
  private BillingProfileModel billingProfile;
  private AzureApplicationDeploymentResource applicationResource;

  @BeforeEach
  void setUp() {
    BillingProfileRequestModel profileRequest =
        ProfileFixtures.randomizeAzureBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);

    applicationResource = ResourceFixtures.randomApplicationDeploymentResource(billingProfile);
    applicationId = azureResourceDao.createApplicationDeployment(applicationResource);
    applicationResource.id(applicationId);
  }

  @Test
  void testCreateEntry() throws Exception {
    UUID datasetId = createDataset("dataset-minimal.json");

    AzureStorageAccountResource storageAccount =
        azureResourceDao.createAndLockStorage(
            "sa",
            datasetId.toString(),
            applicationResource,
            AzureRegion.ASIA_PACIFIC,
            ShortUUID.get());
    datasetStorageAccountDao.createDatasetStorageAccountLink(
        datasetId, storageAccount.getResourceId(), false);

    assertThat(
        "Storage accounts match",
        datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(datasetId),
        equalTo(List.of(storageAccount.getResourceId())));
  }

  private UUID createDataset(String datasetFile) throws Exception {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
    String newName = datasetRequest.getName() + UUID.randomUUID();
    datasetRequest
        .name(newName)
        .defaultProfileId(billingProfile.getId())
        .cloudPlatform(CloudPlatform.AZURE);
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectId);
    dataset.applicationDeploymentResourceId(applicationId);
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    return datasetId;
  }
}
