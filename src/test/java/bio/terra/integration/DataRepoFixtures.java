package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
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
public class DataRepoFixtures {
    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private ObjectMapper objectMapper;


    // studies

    public DataRepoResponse<StudySummaryModel> createStudyRaw(String authToken, String filename) throws Exception {
        StudyRequestModel requestModel = jsonLoader.loadObject(filename, StudyRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            authToken,
            "/api/repository/v1/studies",
            json,
            StudySummaryModel.class);
    }

    public StudySummaryModel createStudy(String authToken, String filename) throws Exception {
        DataRepoResponse<StudySummaryModel> postResponse = createStudyRaw(authToken, filename);
        assertThat("study is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("study create response is present", postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }

    public DataRepoResponse<DeleteResponseModel> deleteStudyRaw(String authToken, String studyId) throws Exception {
        return dataRepoClient.delete(
            authToken, "/api/repository/v1/studies/" + studyId, DeleteResponseModel.class);
    }

    public void deleteStudy(String authToken, String studyId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteStudyRaw(authToken, studyId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<EnumerateStudyModel> enumerateStudiesRaw(String authToken) throws Exception {
        return dataRepoClient.get(authToken, "api/repository/v1/studies", EnumerateStudyModel.class);
    }

    public EnumerateStudyModel enumerateStudies(String authToken) throws Exception {
        DataRepoResponse<EnumerateStudyModel> response = enumerateStudiesRaw(authToken);
        assertThat("study enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<StudyModel> getStudyRaw(String authToken, String studyId) throws Exception {
        return dataRepoClient.get(authToken, "api/repository/v1/studies/" + studyId, StudyModel.class);
    }

    public StudyModel getStudy(String authToken, String studyId) throws Exception {
        DataRepoResponse<StudyModel> response = getStudyRaw(authToken, studyId);
        assertThat("study is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<Object> addStudyPolicyMemberRaw(String authToken,
                                                            String studyId,
                                                            SamClientService.DataRepoRole role,
                                                            String userEmail) throws Exception {
        PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
        return dataRepoClient.post(authToken,
            "api/repository/v1/studies/" + studyId + "/policies/" + role.getPolicyName() + "/members",
            objectMapper.writeValueAsString(req), null);
    }

    public void addStudyPolicyMember(String authToken,
                                     String studyId,
                                     SamClientService.DataRepoRole role,
                                     String userEmail) throws Exception {
        DataRepoResponse<Object> response = addStudyPolicyMemberRaw(authToken, studyId, role, userEmail);
        assertThat("study policy memeber is successfully added", response.getStatusCode(), equalTo(HttpStatus.OK));
    }

    // datasets

    public DataRepoResponse<DatasetSummaryModel> createDatasetRaw(String authToken, StudySummaryModel studySummaryModel,
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

        return dataRepoClient.waitForResponse(authToken, jobResponse, DatasetSummaryModel.class);
    }

    public DatasetSummaryModel createDataset(String authToken, StudySummaryModel studySummaryModel,
                                             String filename) throws Exception {
        DataRepoResponse<DatasetSummaryModel> datasetResponse = createDatasetRaw(
            authToken, studySummaryModel, filename);
        assertThat("dataset create is successful", datasetResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", datasetResponse.getResponseObject().isPresent());
        return datasetResponse.getResponseObject().get();
    }

    public void deleteDataset(String authToken, String datasetId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetRaw(authToken, datasetId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<DeleteResponseModel> deleteDatasetRaw(String authToken, String datasetId) throws Exception {
        DataRepoResponse<JobModel> jobResponse =
            dataRepoClient.delete(authToken, "/api/repository/v1/datasets/" + datasetId, JobModel.class);

        assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, jobResponse, DeleteResponseModel.class);
    }


    private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {
        assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertTrue("Valid delete response", (
            deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    // Create a test study; expect successful creation

    /**
     * Ingests JSON data taking the defaults for the ingest specification
     *
     * @param studyId   - id of study to load
     * @param tableName - name of table to load data into
     * @param datafile  - file path within the bucket from property integrationtest.ingestbucket
     * @return ingest response
     * @throws Exception
     */
    public DataRepoResponse<IngestResponseModel> ingestJsonDataRaw(
        String authToken, String studyId, String tableName, String datafile) throws Exception {
        String ingestBody = buildSimpleIngest(tableName, datafile);
        DataRepoResponse<JobModel> postResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/studies/" + studyId + "/ingest",
            ingestBody,
            JobModel.class);
        assertTrue("ingest launch succeeded", postResponse.getStatusCode().is2xxSuccessful());
        assertTrue("ingest launch response is present", postResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, postResponse, IngestResponseModel.class);
    }

    public IngestResponseModel ingestJsonData(String authToken, String studyId, String tableName, String datafile)
        throws Exception {
        DataRepoResponse<IngestResponseModel> response = ingestJsonDataRaw(authToken, studyId, tableName, datafile);
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

}
