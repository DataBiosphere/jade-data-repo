package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetServiceTest {
  private AuthenticatedUserRequest testUser =
      new AuthenticatedUserRequest().subjectId("DatasetUnit").email("dataset@unit.com");

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private DatasetService datasetService;

  @Autowired private JobService jobService;

  @MockBean private IamProviderInterface samService;

  @Autowired private ConnectedOperations connectedOperations;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private NamedParameterJdbcTemplate jdbcTemplate;

  private BillingProfileModel billingProfile;
  private UUID projectId;
  private ArrayList<String> flightIdsList;
  private ArrayList<UUID> datasetIdList;

  private UUID createDataset(DatasetRequestModel datasetRequest, String newName)
      throws IOException, SQLException {
    datasetRequest
        .name(newName)
        .defaultProfileId(billingProfile.getId())
        .cloudPlatform(CloudPlatform.AZURE);
    Dataset dataset =
        DatasetUtils.convertRequestWithGeneratedNames(datasetRequest).projectResourceId(projectId);
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(datasetId, createFlightId);
    datasetIdList.add(datasetId);
    return datasetId;
  }

  private UUID createDataset(String datasetFile) throws IOException, SQLException {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
    UUID datasetId = createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID());
    datasetIdList.add(datasetId);
    return datasetId;
  }

  @Before
  public void setup() throws Exception {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "hi@hi.hi");
    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);

    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);
    flightIdsList = new ArrayList<>();
    datasetIdList = new ArrayList<>();
  }

  @After
  public void teardown() {
    for (UUID datasetId : datasetIdList) {
      datasetDao.delete(datasetId);
    }
    resourceDao.deleteProject(projectId);
    profileDao.deleteBillingProfileById(billingProfile.getId());
    for (String flightId : flightIdsList) {
      jobService.releaseJob(flightId, testUser);
    }
  }

  @Test
  public void datasetOmopTest() throws IOException, SQLException {
    createDataset("it-dataset-omop.json");
  }

  @Test(expected = DatasetNotFoundException.class)
  public void datasetDeleteTest() throws IOException, SQLException {
    UUID datasetId = createDataset("dataset-create-test.json");
    assertThat("dataset delete signals success", datasetDao.delete(datasetId), equalTo(true));
    datasetDao.retrieve(datasetId);
  }

  @Test
  public void addDatasetAssetSpecifications() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "assetName";
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add asset to dataset
    String jobId = datasetService.addDatasetAssetSpecifications(datasetId, assetModel, testUser);
    flightIdsList.add(jobId);

    TestUtils.eventualExpect(
        5,
        60,
        true,
        () ->
            jobService
                .retrieveJob(jobId, testUser)
                .getJobStatus()
                .equals(JobModel.JobStatusEnum.SUCCEEDED));

    // get dataset
    Dataset dataset = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    datasetDao.delete(datasetId);
  }

  @Test
  public void addMultipleDatasetAssetSpecificationsShouldFail() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "assetName";
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel1 =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    AssetModel assetModel2 =
        new AssetModel()
            .name(assetName)
            .rootTable("participant")
            .rootColumn("id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add first asset to the dataset
    String jobId1 = datasetService.addDatasetAssetSpecifications(datasetId, assetModel1, testUser);
    flightIdsList.add(jobId1);

    boolean assetAdd1 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId1, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertTrue(assetAdd1);

    // get dataset
    Dataset dataset = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    // add second asset to dataset, this should fail because it has the same name as the first
    String jobId2 = datasetService.addDatasetAssetSpecifications(datasetId, assetModel2, testUser);
    flightIdsList.add(jobId2);

    boolean assetAdd2 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId2, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.FAILED));
    Assert.assertTrue(assetAdd2);

    // make sure the first asset we created hasn't been deleted during the undo step
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    datasetDao.delete(datasetId);
  }

  @Test
  public void addAssetSpecWithSameNameToMultipleDatasetsShouldPass() throws Exception {
    UUID datasetId1 = createDataset("dataset-create-test.json");
    UUID datasetId2 = createDataset("dataset-create-test.json");
    String assetName = "assetName";
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId1);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add first asset to the dataset
    String jobId1 = datasetService.addDatasetAssetSpecifications(datasetId1, assetModel, testUser);
    flightIdsList.add(jobId1);

    boolean assetAdd1 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId1, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertTrue(assetAdd1);

    // get dataset 1
    Dataset dataset = datasetDao.retrieve(datasetId1);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    // add asset tp second dataset
    String jobId2 = datasetService.addDatasetAssetSpecifications(datasetId2, assetModel, testUser);
    flightIdsList.add(jobId2);

    boolean assetAdd2 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId2, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertTrue(assetAdd2);

    Dataset dataset2 = datasetDao.retrieve(datasetId2);

    // make sure the second dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset2.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset2.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    datasetDao.delete(datasetId1);
    datasetDao.delete(datasetId2);
  }

  @Test
  public void addDatasetBadAssetSpecification() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "sample"; // This asset name already exists
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add asset to dataset
    String jobId = datasetService.addDatasetAssetSpecifications(datasetId, assetModel, testUser);
    flightIdsList.add(jobId);

    TestUtils.eventualExpect(
        5,
        60,
        true,
        () ->
            jobService
                .retrieveJob(jobId, testUser)
                .getJobStatus()
                .equals(JobModel.JobStatusEnum.FAILED));

    try {
      try {
        jobService.retrieveJobResult(jobId, ErrorModel.class, testUser);
        fail("Expected invalid asset exception");
      } catch (InvalidAssetException ex) {
        assertThat(
            "error message is correct",
            ex.getMessage(),
            equalTo("Asset name already exists: sample"));
        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat(
            "dataset has no additional asset spec",
            dataset.getAssetSpecifications().size(),
            equalTo(2));
      }
    } finally {
      datasetDao.delete(datasetId);
    }
  }

  @Test
  public void removeDatasetAssetSpecifications() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "sample";

    // get dataset
    Dataset datasetWAssets = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has two asset specs already",
        datasetWAssets.getAssetSpecifications().size(),
        equalTo(2));
    assertThat(
        "dataset has expected assets",
        datasetWAssets.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    // remove asset from dataset
    String jobId =
        datasetService.removeDatasetAssetSpecifications(datasetId.toString(), assetName, testUser);
    flightIdsList.add(jobId);

    TestUtils.eventualExpect(
        5,
        60,
        true,
        () ->
            jobService
                .retrieveJob(jobId, testUser)
                .getJobStatus()
                .equals(JobModel.JobStatusEnum.SUCCEEDED));

    // get dataset
    Dataset dataset = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has one less asset spec", dataset.getAssetSpecifications().size(), equalTo(1));

    datasetDao.delete(datasetId);
  }

  @Test
  public void retrieveDatasetDefault() throws SQLException, IOException {
    UUID datasetId = createDataset("dataset-create-test.json");
    Dataset dataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset info defaults are expected",
        datasetService.retrieveModel(dataset),
        equalTo(
            datasetService.retrieveModel(
                dataset,
                List.of(
                    DatasetRequestAccessIncludeModel.SCHEMA,
                    DatasetRequestAccessIncludeModel.PROFILE,
                    DatasetRequestAccessIncludeModel.DATA_PROJECT,
                    DatasetRequestAccessIncludeModel.STORAGE))));
  }
}
