package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudySummaryModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

    private String studyId;
    private List<String> createdDatasetIds;

    @Before
    public void setup() throws Exception {
        String studyJson = jsonLoader.loadJson("ingest-test-study.json");
        DataRepoResponse<StudySummaryModel> postResponse = dataRepoClient.post(
            "/api/repository/v1/studies",
            studyJson,
            StudySummaryModel.class);
        assertThat("study is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("study create response is present", postResponse.getResponseObject().isPresent());
        StudySummaryModel summaryModel = postResponse.getResponseObject().get();
        studyId = summaryModel.getId();
    }

    @After
    public void teardown() throws Exception {
        for (String datasetId : createdDatasetIds) {
            deleteObject("/api/repository/v1/datasets/" + datasetId);
        }

        if (studyId != null) {
            deleteObject("/api/repository/v1/studies/" + studyId);
        }
    }

    private void deleteObject(String endpoint) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse =
            dataRepoClient.delete(endpoint);
        assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertTrue("Valid delete response", (
            deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    @Test
    public void ingestParticipants() throws Exception {
        IngestResponseModel ingestResponse = ingestOne("participant", "ingest-test-participant.json");
        assertThat("no bad participant rows", ingestResponse.getBadRowCount(), equalTo(0L));
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(2L));
    }

    @Test
    public void ingestBuildDataset() throws Exception {
        IngestResponseModel ingestResponse = ingestOne("participant", "ingest-test-participant.json");
        assertThat("no bad participant rows", ingestResponse.getBadRowCount(), equalTo(0L));
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(2L));

        ingestResponse = ingestOne("sample", "ingest-test-sample.json");
        assertThat("no bad sample rows", ingestResponse.getBadRowCount(), equalTo(0L));
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(5L));

        String datasetRequestJson = jsonLoader.loadJson("ingest-test-dataset.json");
        DataRepoResponse<JobModel> jobResponse = dataRepoClient.post(
            "/api/repository/v1/datasets",
            datasetRequestJson,
            JobModel.class);
        assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset create response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DatasetSummaryModel> datasetResponse =
            dataRepoClient.waitForResponse(jobResponse, DatasetSummaryModel.class);
        assertThat("dataset create is successful", datasetResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("ingestOne response is present", datasetResponse.getResponseObject().isPresent());
        DatasetSummaryModel datasetSummary = datasetResponse.getResponseObject().get();
        createdDatasetIds.add(datasetSummary.getId());
    }

    private IngestResponseModel ingestOne(String tableName, String fileName) throws Exception {
        String ingestBody = buildSimpleIngest(tableName, fileName);
        DataRepoResponse<JobModel> postResponse = dataRepoClient.post(
            "/api/repository/v1/studies/" + studyId + "/ingest",
            ingestBody,
            JobModel.class);

        assertTrue("ingest launch succeeded", postResponse.getStatusCode().is2xxSuccessful());
        assertTrue("ingest launch response is present", postResponse.getResponseObject().isPresent());

        DataRepoResponse<IngestResponseModel> response =
            dataRepoClient.waitForResponse(postResponse, IngestResponseModel.class);

        assertThat("ingestOne is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestOne response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }




    private String buildSimpleIngest(String table, String filename) {
        // TODO: REVIEWERS: I think append is better than escaping. Alternate is to include javax.json. Should we???
        StringBuilder ingestBuilder = new StringBuilder()
            .append("{").append('"').append("table").append('"').append(':').append('"').append(table).append('"')
            .append(", ").append('"').append("format").append('"').append(':').append('"').append("json").append('"')
            .append(", ").append('"').append("path").append('"').append(':').append('"')
            .append("gs://").append(dataRepoConfiguration.getIngestbucket()).append("/").append(filename)
            .append('"')
            .append('"').append(':').append('"').append(table).append('"').append('}');

        return ingestBuilder.toString();
    }

}
