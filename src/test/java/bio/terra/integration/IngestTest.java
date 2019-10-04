package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.iam.SamClientService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class IngestTest extends UsersBase {

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private List<String> createdSnapshotIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        super.setup();
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "ingest-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetId, SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
    }

    @After
    public void teardown() throws Exception {
        for (String snapshotId : createdSnapshotIds) {
            dataRepoFixtures.deleteSnapshot(custodian(), snapshotId);
        }

        if (datasetId != null) {
            dataRepoFixtures.deleteDataset(steward(), datasetId);
        }
    }

    @Ignore  // subset of the snapshot test; not worth running everytime, but useful for debugging
    @Test
    public void ingestParticipants() throws Exception {
        IngestRequestModel ingestRequest = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json", IngestRequestModel.StrategyEnum.APPEND);
        IngestResponseModel ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));
    }

    @Test
    public void ingestUpdatedParticipants() throws Exception {
        ingestParticipants();
        IngestRequestModel ingestRequest = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-updated-participant.json", IngestRequestModel.StrategyEnum.UPSERT);
        IngestResponseModel ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
        // FIXME: Ideally we'd be able to assert on the # of rows added, updated, and left unchanged.
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(6L));
    }

    @Test
    public void ingestBuildSnapshot() throws Exception {
        IngestRequestModel ingestRequest = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json", IngestRequestModel.StrategyEnum.APPEND);
        IngestResponseModel ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

        ingestRequest = dataRepoFixtures.buildSimpleIngest(
            "sample", "ingest-test/ingest-test-sample.json", IngestRequestModel.StrategyEnum.APPEND);
        ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

        ingestRequest = dataRepoFixtures.buildSimpleIngest(
            "file", "ingest-test/ingest-test-file.json", IngestRequestModel.StrategyEnum.APPEND);
        ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshot(custodian(), datasetSummaryModel, "ingest-test-snapshot.json");
        createdSnapshotIds.add(snapshotSummary.getId());
    }

    @Test
    public void ingestUnauthorizedTest() throws Exception {
        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json", IngestRequestModel.StrategyEnum.APPEND);
        DataRepoResponse<JobModel> ingestCustResp = dataRepoFixtures.ingestJsonDataLaunch(
            custodian(), datasetId, request);
        assertThat("Custodian is not authorized to ingest data",
            ingestCustResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));
        DataRepoResponse<JobModel> ingestReadResp = dataRepoFixtures.ingestJsonDataLaunch(
                reader(), datasetId, request);
        assertThat("Reader is not authorized to ingest data",
            ingestReadResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    public void ingestAppendNoPkTest() throws Exception {
        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "file", "ingest-test/ingest-test-file.json", IngestRequestModel.StrategyEnum.APPEND);
        IngestResponseModel ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

        ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));
    }

    @Test
    public void ingestUpsertNoPkTest() throws Exception {
        IngestRequestModel requestOne = dataRepoFixtures.buildSimpleIngest(
            "file", "ingest-test/ingest-test-file.json", IngestRequestModel.StrategyEnum.APPEND);
        IngestResponseModel responseOne = dataRepoFixtures.ingestJsonData(steward(), datasetId, requestOne);
        assertThat("correct file row count", responseOne.getRowCount(), equalTo(1L));

        DataRepoResponse<JobModel> upsertJobResponse = dataRepoFixtures.ingestJsonDataLaunch(
            steward(), datasetId, requestOne.strategy(IngestRequestModel.StrategyEnum.UPSERT));
        DataRepoResponse<IngestResponseModel> ingestResponse = dataRepoClient.waitForResponse(
            steward(), upsertJobResponse, IngestResponseModel.class);
        assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
        assertThat("failure is explained",
            ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
            containsString("no primary key"));
    }

    @Test
    public void ingestBadPathTest() throws Exception {
        IngestRequestModel request = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .path("gs://" + testConfig.getIngestbucket() + "/totally-legit-file.json")
            .table("file")
            .strategy(IngestRequestModel.StrategyEnum.APPEND);
        DataRepoResponse<JobModel> ingestJobResponse = dataRepoFixtures.ingestJsonDataLaunch(
            steward(), datasetId, request);
        DataRepoResponse<IngestResponseModel> ingestResponse = dataRepoClient.waitForResponse(
            steward(), ingestJobResponse, IngestResponseModel.class);
        assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.NOT_FOUND));
        assertThat("failure is explained",
            ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
            containsString("file not found"));
    }
}
