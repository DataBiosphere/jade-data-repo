package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class IngestTest extends UsersBase {

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    private StudySummaryModel studySummaryModel;
    private String studyId;
    private List<String> createdDatasetIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        super.setup();
        studySummaryModel = dataRepoFixtures.createStudy(steward(), "ingest-test-study.json");
        studyId = studySummaryModel.getId();
        dataRepoFixtures.addStudyPolicyMember(
            steward(), studyId, SamClientService.DataRepoRole.CUSTODIAN, custodian().getEmail());
    }

    @After
    public void teardown() throws Exception {
        for (String datasetId : createdDatasetIds) {
            dataRepoFixtures.deleteDataset(custodian(), datasetId);
        }

        if (studyId != null) {
            dataRepoFixtures.deleteStudy(steward(), studyId);
        }
    }

    @Ignore  // subset of the dataset test; not worth running everytime, but useful for debugging
    @Test
    public void ingestParticipants() throws Exception {
        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                steward(), studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));
    }

    @Test
    public void ingestBuildDataset() throws Exception {
        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                steward(), studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

        ingestResponse = dataRepoFixtures.ingestJsonData(
            steward(), studyId, "sample", "ingest-test/ingest-test-sample.json");
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

        ingestResponse = dataRepoFixtures.ingestJsonData(
            steward(), studyId, "file", "ingest-test/ingest-test-file.json");
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

        DatasetSummaryModel datasetSummary =
            dataRepoFixtures.createDataset(custodian(), studySummaryModel, "ingest-test-dataset.json");
        createdDatasetIds.add(datasetSummary.getId());
    }

    @Test
    public void ingestUnauthorizedTest() throws Exception {
        DataRepoResponse<JobModel> ingestCustResp = dataRepoFixtures.ingestJsonDataLaunch(
                custodian(), studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("Custodian is not authorized to ingest data",
            ingestCustResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));
        DataRepoResponse<JobModel> ingestReadResp = dataRepoFixtures.ingestJsonDataLaunch(
                reader(), studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("Reader is not authorized to ingest data",
            ingestReadResp.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));
    }

}
