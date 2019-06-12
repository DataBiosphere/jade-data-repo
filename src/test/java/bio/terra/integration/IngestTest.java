package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.auth.Users;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.StudySummaryModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
public class IngestTest {
    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private ValidateTestOperations testOperations;

    @Autowired
    private Users users;

    @Autowired
    private AuthService authService;

    private StudySummaryModel studySummaryModel;
    private String studyId;
    private String stewardToken;
    private String custodianToken;
    private List<String> createdDatasetIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        TestConfiguration.User steward = users.getUserForRole("steward");
        stewardToken = authService.getAuthToken(steward.getEmail());
        TestConfiguration.User custodian = users.getUserForRole("custodian");
        custodianToken = authService.getAuthToken(custodian.getEmail());
        studySummaryModel = testOperations.createTestStudyAndValidate(stewardToken, "ingest-test-study.json");
        studyId = studySummaryModel.getId();
    }

    @After
    public void teardown() throws Exception {
        for (String datasetId : createdDatasetIds) {
            testOperations.deleteTestDataset(custodianToken, datasetId);
        }

        if (studyId != null) {
            testOperations.deleteTestStudy(stewardToken, studyId);
        }
    }

    @Ignore  // subset of the dataset test; not worth running everytime, but useful for debugging
    @Test
    public void ingestParticipants() throws Exception {
        IngestResponseModel ingestResponse =
            testOperations.ingestJsonData(
                stewardToken, studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));
    }

    @Test
    public void ingestBuildDataset() throws Exception {
        IngestResponseModel ingestResponse =
            testOperations.ingestJsonData(
                stewardToken, studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(2L));

        ingestResponse = testOperations.ingestJsonData(
            stewardToken, studyId, "sample", "ingest-test/ingest-test-sample.json");
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(5L));

        ingestResponse = testOperations.ingestJsonData(
            stewardToken, studyId, "file", "ingest-test/ingest-test-file.json");
        assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

        DatasetSummaryModel datasetSummary =
            testOperations.createTestDataset(custodianToken, studySummaryModel, "ingest-test-dataset.json");
        createdDatasetIds.add(datasetSummary.getId());
    }

}
