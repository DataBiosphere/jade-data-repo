package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
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
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.fixtures.ProfileFixtures;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
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

    @Autowired
    private GoogleResourceConfiguration googleResourceConfiguration;

    // Create a Billing Profile model: expect successful creation
    public BillingProfileModel createBillingProfile(TestConfiguration.User user) throws Exception {
        BillingProfileRequestModel billingProfileRequestModel = ProfileFixtures.randomBillingProfileRequest();
        String json = objectMapper.writeValueAsString(billingProfileRequestModel);
        DataRepoResponse<BillingProfileModel> postResponse = dataRepoClient.post(
            user,
            "/api/resources/v1/profiles",
            json,
            BillingProfileModel.class);

        assertThat("billing profile model is successfuly created", postResponse.getStatusCode(),
            equalTo(HttpStatus.CREATED));
        assertTrue("create billing profile model response is present",
            postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }
    // studies

    public DataRepoResponse<StudySummaryModel> createStudyRaw(TestConfiguration.User user, String filename)
        throws Exception {
        StudyRequestModel requestModel = jsonLoader.loadObject(filename, StudyRequestModel.class);
        BillingProfileModel billingProfileModel = this.createBillingProfile(user);
        requestModel.setDefaultProfileId(billingProfileModel.getId());
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/studies",
            json,
            StudySummaryModel.class);
    }

    public StudySummaryModel createStudy(TestConfiguration.User user, String filename) throws Exception {
        DataRepoResponse<StudySummaryModel> postResponse = createStudyRaw(user, filename);
        assertThat("study is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("study create response is present", postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }

    public DataRepoResponse<DeleteResponseModel> deleteStudyRaw(TestConfiguration.User user, String studyId)
        throws Exception {
        return dataRepoClient.delete(
            user, "/api/repository/v1/studies/" + studyId, DeleteResponseModel.class);
    }

    public void deleteStudy(TestConfiguration.User user, String studyId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteStudyRaw(user, studyId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<EnumerateStudyModel> enumerateStudiesRaw(TestConfiguration.User user) throws Exception {
        return dataRepoClient.get(user,
            "/api/repository/v1/studies?sort=created_date&direction=desc",
            EnumerateStudyModel.class);
    }

    public EnumerateStudyModel enumerateStudies(TestConfiguration.User user) throws Exception {
        DataRepoResponse<EnumerateStudyModel> response = enumerateStudiesRaw(user);
        assertThat("study enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<StudyModel> getStudyRaw(TestConfiguration.User user, String studyId) throws Exception {
        return dataRepoClient.get(user, "/api/repository/v1/studies/" + studyId, StudyModel.class);
    }

    public StudyModel getStudy(TestConfiguration.User user, String studyId) throws Exception {
        DataRepoResponse<StudyModel> response = getStudyRaw(user, studyId);
        assertThat("study is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<Object> addPolicyMemberRaw(TestConfiguration.User user,
                                                       String resourceId,
                                                       SamClientService.DataRepoRole role,
                                                       String userEmail,
                                                       SamClientService.ResourceType resourceType) throws Exception {
        PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
        return dataRepoClient.post(user, "/api/repository/v1/" + resourceType.toPluralString() + "/" +
                resourceId + "/policies/" + role.toString() + "/members",
            objectMapper.writeValueAsString(req), null);
    }

    public void addPolicyMember(TestConfiguration.User user,
                                String resourceId,
                                SamClientService.DataRepoRole role,
                                String userEmail,
                                SamClientService.ResourceType resourceType) throws Exception {
        DataRepoResponse<Object> response = addPolicyMemberRaw(user, resourceId, role, userEmail, resourceType);
        assertThat(resourceType + " policy member is successfully added",
            response.getStatusCode(), equalTo(HttpStatus.OK));
    }


    // adding study policy
    public void addStudyPolicyMember(TestConfiguration.User user,
                                     String studyId,
                                     SamClientService.DataRepoRole role,
                                     String userEmail) throws Exception {
        addPolicyMember(user, studyId, role, userEmail, SamClientService.ResourceType.STUDY);
    }

    // datasets

    // adding dataset policy
    public void addDatasetPolicyMember(TestConfiguration.User user,
                                       String datasetId,
                                       SamClientService.DataRepoRole role,
                                       String userEmail) throws Exception {
        addPolicyMember(user, datasetId, role, userEmail, SamClientService.ResourceType.DATASET);
    }

    public DataRepoResponse<JobModel> createDatasetLaunch(
        TestConfiguration.User user, StudySummaryModel studySummaryModel, String filename) throws Exception {
        DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
        BillingProfileModel billingProfileModel = this.createBillingProfile(user);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        requestModel.getContents().get(0).getSource().setStudyName(studySummaryModel.getName());
        requestModel.setProfileId(billingProfileModel.getId());
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/datasets",
            json,
            JobModel.class);
    }

    public DatasetSummaryModel createDataset(
        TestConfiguration.User user, StudySummaryModel studySummaryModel, String filename) throws Exception {
        DataRepoResponse<JobModel> jobResponse = createDatasetLaunch(
            user, studySummaryModel, filename);
        assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset create launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DatasetSummaryModel> datasetResponse = dataRepoClient.waitForResponse(
            user, jobResponse, DatasetSummaryModel.class);
        assertThat("dataset create is successful", datasetResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", datasetResponse.getResponseObject().isPresent());
        return datasetResponse.getResponseObject().get();
    }

    public DataRepoResponse<DatasetModel> getDataDatasetRaw(TestConfiguration.User user, String datasetId)
        throws Exception {
        return dataRepoClient.get(user, "/api/repository/v1/datasets/" + datasetId, DatasetModel.class);
    }

    public DatasetModel getDataset(TestConfiguration.User user, String datasetId) throws Exception {
        DataRepoResponse<DatasetModel> response = getDataDatasetRaw(user, datasetId);
        assertThat("study is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<EnumerateDatasetModel> enumerateDatasetsRaw(TestConfiguration.User user) throws Exception {
        return dataRepoClient.get(user,
            "/api/repository/v1/datasets?sort=created_date&direction=desc",
            EnumerateDatasetModel.class);
    }

    public EnumerateDatasetModel enumerateDatasets(TestConfiguration.User user) throws Exception {
        DataRepoResponse<EnumerateDatasetModel> response = enumerateDatasetsRaw(user);
        assertThat("dataset enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("dataset get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public void deleteDataset(TestConfiguration.User user, String datasetId) throws Exception {
        DataRepoResponse<JobModel> jobResponse = deleteDatasetLaunch(user, datasetId);
        assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoClient.waitForResponse(
            user, jobResponse, DeleteResponseModel.class);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<JobModel> deleteDatasetLaunch(TestConfiguration.User user, String datasetId)
        throws Exception {
        return dataRepoClient.delete(user, "/api/repository/v1/datasets/" + datasetId, JobModel.class);
    }


    private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {
        assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertTrue("Valid delete response", (
            deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }


    /**
     * Ingests JSON data taking the defaults for the ingest specification
     *
     * @param studyId   - id of study to load
     * @param tableName - name of table to load data into
     * @param datafile  - file path within the bucket from property integrationtest.ingestbucket
     * @return ingest response
     * @throws Exception
     */
    public DataRepoResponse<JobModel> ingestJsonDataLaunch(
        TestConfiguration.User user, String studyId, String tableName, String datafile) throws Exception {
        String ingestBody = buildSimpleIngest(tableName, datafile);
        return dataRepoClient.post(
            user,
            "/api/repository/v1/studies/" + studyId + "/ingest",
            ingestBody,
            JobModel.class);
    }

    public IngestResponseModel ingestJsonData(
        TestConfiguration.User user, String studyId, String tableName, String datafile) throws Exception {
        DataRepoResponse<JobModel> launchResp = ingestJsonDataLaunch(user, studyId, tableName, datafile);
        assertTrue("ingest launch succeeded", launchResp.getStatusCode().is2xxSuccessful());
        assertTrue("ingest launch response is present", launchResp.getResponseObject().isPresent());
        DataRepoResponse<IngestResponseModel> response = dataRepoClient.waitForResponse(
            user, launchResp, IngestResponseModel.class);

        assertThat("ingestOne is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestOne response is present", response.getResponseObject().isPresent());

        IngestResponseModel ingestResponse = response.getResponseObject().get();
        assertThat("no bad sample rows", ingestResponse.getBadRowCount(), equalTo(0L));
        return ingestResponse;
    }

    public DataRepoResponse<FSObjectModel> ingestFileRaw(
        TestConfiguration.User user, String studyId, String sourceGsPath, String targetPath) throws Exception {

        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(sourceGsPath)
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(targetPath);

        String json = objectMapper.writeValueAsString(fileLoadModel);

        DataRepoResponse<JobModel> postResponse = dataRepoClient.post(
            user,
            "/api/repository/v1/studies/" + studyId + "/files",
            json,
            JobModel.class);
        return dataRepoClient.waitForResponse(user, postResponse, FSObjectModel.class);
    }

    public FSObjectModel ingestFile(
        TestConfiguration.User user, String studyId, String sourceGsPath, String targetPath) throws Exception {
        DataRepoResponse<FSObjectModel> response = ingestFileRaw(user, studyId, sourceGsPath, targetPath);

        assertThat("ingestFile is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestFile response is present", response.getResponseObject().isPresent());

        FSObjectModel ingestResponse = response.getResponseObject().get();
        return ingestResponse;
    }

    public DataRepoResponse<DeleteResponseModel> deleteFileRaw(
        TestConfiguration.User user, String studyId, String fileId) throws Exception {

        DataRepoResponse<JobModel> deleteResponse = dataRepoClient.delete(
            user,
            "/api/repository/v1/studies/" + studyId + "/files/" + fileId,
            JobModel.class);
        return dataRepoClient.waitForResponse(user, deleteResponse, DeleteResponseModel.class);
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
