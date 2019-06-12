package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Component
@Profile("integrationtest")
public class ValidateTestOperations {
    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataRepoFixtures dataRepoFixtures;


    // Create a test study; expect successful creation
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
