package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
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
        return dataRepoClient.get(authToken,
            "/api/repository/v1/studies?sort=created_date&direction=desc",
            EnumerateStudyModel.class);
    }

    public EnumerateStudyModel enumerateStudies(String authToken) throws Exception {
        DataRepoResponse<EnumerateStudyModel> response = enumerateStudiesRaw(authToken);
        assertThat("study enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<StudyModel> getStudyRaw(String authToken, String studyId) throws Exception {
        return dataRepoClient.get(authToken, "/api/repository/v1/studies/" + studyId, StudyModel.class);
    }

    public StudyModel getStudy(String authToken, String studyId) throws Exception {
        DataRepoResponse<StudyModel> response = getStudyRaw(authToken, studyId);
        assertThat("study is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<Object> addPolicyMemberRaw(String authToken,
                                                            String resourceId,
                                                            SamClientService.DataRepoRole role,
                                                            String userEmail,
                                                            SamClientService.ResourceType resourceType) throws Exception {
        PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
        return dataRepoClient.post(authToken,
            "/api/repository/v1/"+resourceType.toPluralString()+"/" + resourceId + "/policies/" + role.toString() + "/members",
            objectMapper.writeValueAsString(req), null);
    }

    public void addPolicyMember(String authToken,
                                     String resourceId,
                                     SamClientService.DataRepoRole role,
                                     String userEmail,
                                     SamClientService.ResourceType resourceType) throws Exception {
        DataRepoResponse<Object> response = addPolicyMemberRaw(authToken, resourceId, role, userEmail, resourceType);
        assertThat(resourceType + " policy member is successfully added",
            response.getStatusCode(), equalTo(HttpStatus.OK));
    }


    // adding study policy
    public void addStudyPolicyMember(String authToken,
                                String studyId,
                                SamClientService.DataRepoRole role,
                                String userEmail) throws Exception {
        addPolicyMember(authToken, studyId, role, userEmail, SamClientService.ResourceType.STUDY);
    }

    // datasets

    // adding dataset policy
    public void addDatasetPolicyMember(String authToken,
                                     String datasetId,
                                     SamClientService.DataRepoRole role,
                                     String userEmail) throws Exception {
        addPolicyMember(authToken, datasetId, role, userEmail, SamClientService.ResourceType.DATASET);
    }

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

    public DataRepoResponse<FSObjectModel> ingestFileRaw(
        String authToken, String studyId, String sourceGsPath, String targetPath) throws Exception {

        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(sourceGsPath)
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(targetPath);

        String json = objectMapper.writeValueAsString(fileLoadModel);

        DataRepoResponse<JobModel> postResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/studies/" + studyId + "/files",
            json,
            JobModel.class);
        return dataRepoClient.waitForResponse(authToken, postResponse, FSObjectModel.class);
    }

    public FSObjectModel ingestFile(
        String authToken, String studyId, String sourceGsPath, String targetPath) throws Exception {
        DataRepoResponse<FSObjectModel> response = ingestFileRaw(authToken, studyId, sourceGsPath, targetPath);

        assertThat("ingestFile is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestFile response is present", response.getResponseObject().isPresent());

        FSObjectModel ingestResponse = response.getResponseObject().get();
        return ingestResponse;
    }

    public DataRepoResponse<DeleteResponseModel> deleteFileRaw(
        String authToken, String studyId, String fileId) throws Exception {

        DataRepoResponse<JobModel> deleteResponse = dataRepoClient.delete(
            authToken,
            "/api/repository/v1/studies/" + studyId + "/files/" + fileId,
            JobModel.class);
        return dataRepoClient.waitForResponse(authToken, deleteResponse, DeleteResponseModel.class);
    }

    private String buildSimpleIngest(String table, String filename) throws Exception {
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + filename;
        IngestRequestModel ingestRequest = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table(table)
            .path(gsPath);
        return objectMapper.writeValueAsString(ingestRequest);
    }

}
