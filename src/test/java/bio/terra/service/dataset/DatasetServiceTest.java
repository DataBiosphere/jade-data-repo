package bio.terra.service.dataset;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.ProfileDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetServiceTest {
    private AuthenticatedUserRequest testUser = new AuthenticatedUserRequest()
        .subjectId("DatasetUnit")
        .email("dataset@unit.com");

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private JobService jobService;

    @MockBean
    private IamService samService;

    @Autowired
    private ConnectedOperations connectedOperations;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private BillingProfile billingProfile;

    private ArrayList<String> flightIdsList;

    private ArrayList<UUID> datasetIdList;

    private UUID createDataset(DatasetRequestModel datasetRequest, String newName) throws IOException, SQLException {
        datasetRequest.name(newName).defaultProfileId(billingProfile.getId().toString());
        Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
        String createFlightId = UUID.randomUUID().toString();
        UUID datasetId = datasetDao.createAndLock(dataset, createFlightId);
        datasetDao.unlock(dataset.getId(), createFlightId);
        datasetIdList.add(datasetId);
        return datasetId;
    }

    private UUID createDataset(String datasetFile) throws IOException, SQLException {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        UUID datasetId = createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID().toString());
        datasetIdList.add(datasetId);
        return datasetId;
    }

    @Before
    public void setup() {
        billingProfile = ProfileFixtures.randomBillingProfile();
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);
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
        assertThat("dataset already has two asset specs", createdDataset.getAssetSpecifications().size(), equalTo(2));

        AssetModel assetModel = new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(Arrays.asList(
                DatasetFixtures.buildAssetParticipantTable(),
                DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

        // add asset to dataset
        String jobId = datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel, testUser);
        flightIdsList.add(jobId);

        TestUtils.eventualExpect(5, 60, true, () ->
            jobService.retrieveJob(jobId, testUser).getJobStatus().equals(JobModel.JobStatusEnum.SUCCEEDED)
        );

        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat("dataset has an additional asset spec", dataset.getAssetSpecifications().size(), equalTo(3));
        assertThat("dataset has expected asset", dataset.getAssetSpecificationByName(assetName).isPresent(),
            equalTo(true));

        datasetDao.delete(datasetId);
    }

    @Test
    public void addDatasetBadAssetSpecification() throws Exception {
        UUID datasetId = createDataset("dataset-create-test.json");
        String assetName = "sample"; // This asset name already exists
        // get created dataset
        Dataset createdDataset = datasetDao.retrieve(datasetId);
        assertThat("dataset already has two asset specs", createdDataset.getAssetSpecifications().size(), equalTo(2));

        AssetModel assetModel = new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(Arrays.asList(
                DatasetFixtures.buildAssetParticipantTable(),
                DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

        // add asset to dataset
        String jobId = datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel, testUser);
        flightIdsList.add(jobId);

        TestUtils.eventualExpect(5, 60, true, () ->
            jobService.retrieveJob(jobId, testUser).getJobStatus().equals(JobModel.JobStatusEnum.SUCCEEDED)
        );

        JobService.JobResultWithStatus<ErrorModel> resultWithStatus =
            jobService.retrieveJobResult(jobId, ErrorModel.class, testUser);

        assertThat("error message is correct", resultWithStatus.getResult().getMessage(),
            equalTo("Asset already exists: sample"));
        assertThat("error status is correct", resultWithStatus.getStatusCode(),
            equalTo(HttpStatus.BAD_REQUEST));

        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat("dataset has no additional asset spec", dataset.getAssetSpecifications().size(), equalTo(2));
        datasetDao.delete(datasetId);
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
        assertThat("dataset has expected assets", datasetWAssets.getAssetSpecificationByName(assetName).isPresent(),
            equalTo(true));

        // remove asset from dataset
        String jobId = datasetService.removeDatasetAssetSpecifications(datasetId.toString(), assetName, testUser);
        flightIdsList.add(jobId);

        TestUtils.eventualExpect(5, 60, true, () ->
            jobService.retrieveJob(jobId, testUser).getJobStatus().equals(JobModel.JobStatusEnum.SUCCEEDED)
        );

        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat("dataset has one less asset spec", dataset.getAssetSpecifications().size(), equalTo(1));

        datasetDao.delete(datasetId);
    }
}
