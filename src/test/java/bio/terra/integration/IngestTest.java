package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.SamClientService;
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
        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                steward(), datasetId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));
    }

    @Test
    public void ingestUpdatedParticipants() throws Exception {
        ingestParticipants();
        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                steward(),
                datasetId,
                "participant",
                "ingest-test/ingest-test-updated-participant.json",
                IngestRequestModel.StrategyEnum.UPSERT);
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(6L));
    }

    @Test
    public void ingestBuildSnapshot() throws Exception {
        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                steward(), datasetId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

        ingestResponse = dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "sample", "ingest-test/ingest-test-sample.json");
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

        ingestResponse = dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "file", "ingest-test/ingest-test-file.json");
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

        SnapshotSummaryModel snapshotSummary =
            dataRepoFixtures.createSnapshot(custodian(), datasetSummaryModel, "ingest-test-snapshot.json");
        createdSnapshotIds.add(snapshotSummary.getId());
    }

    @Test
    public void ingestUnauthorizedTest() throws Exception {
        DataRepoResponse<JobModel> ingestCustResp = dataRepoFixtures.ingestJsonDataLaunch(
                custodian(), datasetId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("Custodian is not authorized to ingest data",
            ingestCustResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));
        DataRepoResponse<JobModel> ingestReadResp = dataRepoFixtures.ingestJsonDataLaunch(
                reader(), datasetId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("Reader is not authorized to ingest data",
            ingestReadResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    public void ingestAppendNoPkTest() throws Exception {
        IngestResponseModel ingestResponse = dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "file", "ingest-test/ingest-test-file.json");
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));
        ingestResponse = dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "file", "ingest-test/ingest-test-file.json");
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(2L));
    }

    @Test
    public void ingestUpsertNoPkTest() throws Exception {
        IngestResponseModel ingestOne = dataRepoFixtures.ingestJsonData(
            steward(), datasetId, "file", "ingest-test/ingest-test-file.json");
        assertThat("correct file row count", ingestOne.getRowCount(), equalTo(1L));
        DataRepoResponse<JobModel> upsertJobResponse = dataRepoFixtures.ingestJsonDataLaunch(
            steward(), datasetId, "file", "ingest-test/ingest-test-file.json",
            IngestRequestModel.StrategyEnum.UPSERT);
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
            .table("file");
        DataRepoResponse<JobModel> ingestJobResponse = dataRepoFixtures.ingestJsonDataLaunch(
            steward(), datasetId, request);
        DataRepoResponse<IngestResponseModel> ingestResponse = dataRepoClient.waitForResponse(
            steward(), ingestJobResponse, IngestResponseModel.class);
        assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
        assertThat("failure is explained",
            ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
            containsString("file not found"));
    }
}
