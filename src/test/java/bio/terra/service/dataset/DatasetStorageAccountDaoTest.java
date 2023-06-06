package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class DatasetStorageAccountDaoTest {
  private static final Logger logger = LoggerFactory.getLogger(DatasetStorageAccountDaoTest.class);
  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private DatasetStorageAccountDao datasetStorageAccountDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private AzureResourceDao azureResourceDao;

  private List<UUID> billingProfileIds = new ArrayList<>();
  private List<UUID> datasetIds = new ArrayList<>();
  private List<UUID> storageAccountResourceIds = new ArrayList<>();

  private UUID applicationId;
  private UUID projectId;
  private Dataset dataset;
  private BillingProfileModel billingProfile;
  private AzureApplicationDeploymentResource applicationResource;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  @Before
  public void setUp() throws Exception {
    BillingProfileRequestModel profileRequest =
        ProfileFixtures.randomizeAzureBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");
    billingProfileIds.add(billingProfile.getId());

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);

    applicationResource = ResourceFixtures.randomApplicationDeploymentResource(billingProfile);
    applicationId = azureResourceDao.createApplicationDeployment(applicationResource);
    applicationResource.id(applicationId);
  }

  @After
  public void teardown() {
    for (UUID datasetId : datasetIds) {
      datasetStorageAccountDao.deleteDatasetStorageAccountLink(
          datasetId, storageAccountResourceIds.get(0));

      datasetDao.delete(datasetId);
    }

    azureResourceDao.deleteApplicationDeploymentMetadata(List.of(applicationId));
  }

  @Test
  public void testCreateEntry() throws Exception {
    UUID datasetId = createDataset("dataset-minimal.json");
    datasetIds.add(datasetId);

    AzureStorageAccountResource storageAccount =
        azureResourceDao.createAndLockStorage(
            "sa",
            datasetId.toString(),
            applicationResource,
            AzureRegion.ASIA_PACIFIC,
            ShortUUID.get());
    storageAccountResourceIds.add(storageAccount.getResourceId());
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
    dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectId);
    dataset.applicationDeploymentResourceId(applicationId);
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId, TEST_USER);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    return datasetId;
  }
}
