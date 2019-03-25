package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
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
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(Integration.class)
public class IngestTest {
    @Autowired
    private DataRepoConfiguration dataRepoConfiguration;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private TestOperations testOperations;

    private StudySummaryModel studySummaryModel;
    private String studyId;
    private List<String> createdDatasetIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        studySummaryModel = testOperations.createTestStudy("ingest-test-study.json");
        studyId = studySummaryModel.getId();
    }

    @After
    public void teardown() throws Exception {
        for (String datasetId : createdDatasetIds) {
            testOperations.deleteTestDataset(datasetId);
        }

        if (studyId != null) {
            testOperations.deleteTestStudy(studyId);
        }
    }

    @Ignore  // subset of the dataset test; not worth running everytime, but useful for debugging
    @Test
    public void ingestParticipants() throws Exception {
        IngestResponseModel ingestResponse =
            testOperations.ingestJsonData(studyId, "participant", "ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(2L));
    }

    @Test
    public void ingestBuildDataset() throws Exception {
        IngestResponseModel ingestResponse =
            testOperations.ingestJsonData(studyId, "participant", "ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(2L));

        ingestResponse = testOperations.ingestJsonData(studyId, "sample", "ingest-test-sample.json");
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(5L));

        DatasetSummaryModel datasetSummary =
            testOperations.createTestDataset(studySummaryModel, "ingest-test-dataset.json");
        createdDatasetIds.add(datasetSummary.getId());
    }

}
