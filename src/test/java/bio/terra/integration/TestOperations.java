package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Component
@ActiveProfiles({"google", "integrationtest"})
public class TestOperations {
    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private ObjectMapper objectMapper;

    // Create a test study; expect successful creation
    public StudySummaryModel createTestStudy(String authToken, String filename) throws Exception {
        StudyRequestModel requestModel = jsonLoader.loadObject(filename, StudyRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = objectMapper.writeValueAsString(requestModel);

        DataRepoResponse<StudySummaryModel> postResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/studies",
            json,
            StudySummaryModel.class);
        assertThat("study is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("study create response is present", postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }

    public void deleteTestStudy(String authToken, String studyId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse =
            dataRepoClient.delete(authToken, "/api/repository/v1/studies/" + studyId, DeleteResponseModel.class);
        assertGoodDeleteResponse(deleteResponse);
    }

    // Create a test dataset; expect successful creation
    public DatasetSummaryModel createTestDataset(String authToken, StudySummaryModel studySummaryModel,
                                                 String filename) throws Exception {
        DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        requestModel.getContents().get(0).getSource().setStudyName(studySummaryModel.getName());
        String json = objectMapper.writeValueAsString(requestModel);

        DataRepoResponse<JobModel> jobResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/datasets",
            json,
            JobModel.class);
        assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset create launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DatasetSummaryModel> datasetResponse =
            dataRepoClient.waitForResponse(authToken, jobResponse, DatasetSummaryModel.class);
        assertThat("dataset create is successful", datasetResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", datasetResponse.getResponseObject().isPresent());
        return datasetResponse.getResponseObject().get();
    }

    public void deleteTestDataset(String authToken, String datasetId) throws Exception {
        DataRepoResponse<JobModel> jobResponse =
            dataRepoClient.delete(authToken, "/api/repository/v1/datasets/" + datasetId, JobModel.class);

        assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DeleteResponseModel> deleteResponse =
            dataRepoClient.waitForResponse(authToken, jobResponse, DeleteResponseModel.class);
        assertGoodDeleteResponse(deleteResponse);
    }

    /**
     * Ingests JSON data taking the defaults for the ingest specification
     *
     * @param studyId    - id of study to load
     * @param tableName  - name of table to load data into
     * @param datafile   - file path within the bucket from property integrationtest.ingestbucket
     * @return ingest response
     * @throws Exception
     */
    public IngestResponseModel ingestJsonData(String authToken, String studyId, String tableName, String datafile)
        throws Exception {
        String ingestBody = buildSimpleIngest(tableName, datafile);
        DataRepoResponse<JobModel> postResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/studies/" + studyId + "/ingest",
            ingestBody,
            JobModel.class);

        assertTrue("ingest launch succeeded", postResponse.getStatusCode().is2xxSuccessful());
        assertTrue("ingest launch response is present", postResponse.getResponseObject().isPresent());

        DataRepoResponse<IngestResponseModel> response =
            dataRepoClient.waitForResponse(authToken, postResponse, IngestResponseModel.class);

        assertThat("ingestOne is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestOne response is present", response.getResponseObject().isPresent());

        IngestResponseModel ingestResponse = response.getResponseObject().get();
        assertThat("no bad sample rows", ingestResponse.getBadRowCount(), equalTo(0L));
        return ingestResponse;
    }

    private String buildSimpleIngest(String table, String filename) {
        // TODO: Change this to create the IngestRequestModel and convert it to JSON
        StringBuilder ingestBuilder = new StringBuilder()
            .append("{").append('"').append("table").append('"').append(':').append('"').append(table).append('"')
            .append(", ").append('"').append("format").append('"').append(':').append('"').append("json").append('"')
            .append(", ").append('"').append("path").append('"').append(':').append('"')
            .append("gs://").append(testConfig.getIngestbucket()).append("/").append(filename)
            .append('"').append('}');

        return ingestBuilder.toString();
    }

    private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {
        assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertTrue("Valid delete response", (
            deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

}
