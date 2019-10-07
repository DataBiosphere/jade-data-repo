package bio.terra.integration;

import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.FileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Component
public class DataRepoFixtures {

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private ObjectMapper objectMapper;

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

    // datasets

    public DataRepoResponse<DatasetSummaryModel> createDatasetRaw(TestConfiguration.User user, String filename)
        throws Exception {
        DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
        BillingProfileModel billingProfileModel = this.createBillingProfile(user);
        requestModel.setDefaultProfileId(billingProfileModel.getId());
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/datasets",
            json,
            DatasetSummaryModel.class);
    }

    public DatasetSummaryModel createDataset(TestConfiguration.User user, String filename) throws Exception {
        DataRepoResponse<DatasetSummaryModel> postResponse = createDatasetRaw(user, filename);
        assertThat("dataset is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }

    public DataRepoResponse<DeleteResponseModel> deleteDatasetRaw(TestConfiguration.User user, String datasetId)
        throws Exception {
        return dataRepoClient.delete(
            user, "/api/repository/v1/datasets/" + datasetId, DeleteResponseModel.class);
    }

    public void deleteDataset(TestConfiguration.User user, String datasetId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetRaw(user, datasetId);
        assertGoodDeleteResponse(deleteResponse);
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

    public DataRepoResponse<DatasetModel> getDatasetRaw(
        TestConfiguration.User user,
        String datasetId) throws Exception {
        return dataRepoClient.get(user, "/api/repository/v1/datasets/" + datasetId, DatasetModel.class);
    }

    public DatasetModel getDataset(TestConfiguration.User user, String datasetId) throws Exception {
        DataRepoResponse<DatasetModel> response = getDatasetRaw(user, datasetId);
        assertThat("dataset is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("dataset get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<Object> addPolicyMemberRaw(TestConfiguration.User user,
                                                       String resourceId,
                                                       SamClientService.DataRepoRole role,
                                                       String userEmail,
                                                       SamClientService.ResourceType resourceType) throws Exception {
        PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
        return dataRepoClient.post(user, "/api/repository/v1/" + TestUtils.getHttpPathString(resourceType) + "/" +
                resourceId + "/policies/" + role.toString() + "/members",
            objectMapper.writeValueAsString(req), null);
    }

    public void addPolicyMember(TestConfiguration.User user,
                                String resourceId,
                                SamClientService.DataRepoRole role,
                                String newMemberEmail,
                                SamClientService.ResourceType resourceType) throws Exception {
        DataRepoResponse<Object> response = addPolicyMemberRaw(user, resourceId, role, newMemberEmail, resourceType);
        assertThat(resourceType + " policy member is successfully added",
            response.getStatusCode(), equalTo(HttpStatus.OK));
    }


    // adding dataset policy
    public void addDatasetPolicyMember(TestConfiguration.User user,
                                     String datasetId,
                                     SamClientService.DataRepoRole role,
                                     String newMemberEmail) throws Exception {
        addPolicyMember(user, datasetId, role, newMemberEmail, SamClientService.ResourceType.DATASET);
    }

    // snapshots

    // adding snapshot policy
    public void addSnapshotPolicyMember(TestConfiguration.User user,
                                       String snapshotId,
                                       SamClientService.DataRepoRole role,
                                       String newMemberEmail) throws Exception {
        addPolicyMember(user, snapshotId, role, newMemberEmail, SamClientService.ResourceType.DATASNAPSHOT);
    }

    public DataRepoResponse<JobModel> createSnapshotLaunch(
        TestConfiguration.User user, DatasetSummaryModel datasetSummaryModel, String filename) throws Exception {
        SnapshotRequestModel requestModel = jsonLoader.loadObject(filename, SnapshotRequestModel.class);
        BillingProfileModel billingProfileModel = this.createBillingProfile(user);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        requestModel.getContents().get(0).getSource().setDatasetName(datasetSummaryModel.getName());
        requestModel.setProfileId(billingProfileModel.getId());
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/snapshots",
            json,
            JobModel.class);
    }

    public SnapshotSummaryModel createSnapshot(
        TestConfiguration.User user, DatasetSummaryModel datasetSummaryModel, String filename) throws Exception {
        DataRepoResponse<JobModel> jobResponse = createSnapshotLaunch(
            user, datasetSummaryModel, filename);
        assertTrue("snapshot create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("snapshot create launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<SnapshotSummaryModel> snapshotResponse = dataRepoClient.waitForResponse(
            user, jobResponse, SnapshotSummaryModel.class);
        assertThat("snapshot create is successful", snapshotResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("snapshot create response is present", snapshotResponse.getResponseObject().isPresent());
        return snapshotResponse.getResponseObject().get();
    }

    public DataRepoResponse<SnapshotModel> getSnapshotRaw(TestConfiguration.User user, String snapshotId)
        throws Exception {
        return dataRepoClient.get(user, "/api/repository/v1/snapshots/" + snapshotId, SnapshotModel.class);
    }

    public SnapshotModel getSnapshot(TestConfiguration.User user, String snapshotId) throws Exception {
        DataRepoResponse<SnapshotModel> response = getSnapshotRaw(user, snapshotId);
        assertThat("dataset is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("dataset get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<EnumerateSnapshotModel> enumerateSnapshotsRaw(
        TestConfiguration.User user) throws Exception {
        return dataRepoClient.get(user,
            "/api/repository/v1/snapshots?sort=created_date&direction=desc",
            EnumerateSnapshotModel.class);
    }

    public EnumerateSnapshotModel enumerateSnapshots(TestConfiguration.User user) throws Exception {
        DataRepoResponse<EnumerateSnapshotModel> response = enumerateSnapshotsRaw(user);
        assertThat("snapshot enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("snapshot get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public void deleteSnapshot(TestConfiguration.User user, String snapshotId) throws Exception {
        DataRepoResponse<JobModel> jobResponse = deleteSnapshotLaunch(user, snapshotId);
        assertTrue("snapshot delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("snapshot delete launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoClient.waitForResponse(
            user, jobResponse, DeleteResponseModel.class);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<JobModel> deleteSnapshotLaunch(TestConfiguration.User user, String snapshotId)
        throws Exception {
        return dataRepoClient.delete(user, "/api/repository/v1/snapshots/" + snapshotId, JobModel.class);
    }


    private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {
        assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertTrue("Valid delete response", (
            deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    public DataRepoResponse<JobModel> ingestJsonDataLaunch(
        TestConfiguration.User user, String datasetId, IngestRequestModel request) throws Exception {
        String ingestBody = objectMapper.writeValueAsString(request);
        return dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/ingest",
            ingestBody,
            JobModel.class);
    }

    public IngestResponseModel ingestJsonData(
        TestConfiguration.User user, String datasetId, IngestRequestModel request) throws Exception {

        DataRepoResponse<JobModel> launchResp = ingestJsonDataLaunch(user, datasetId, request);
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

    public DataRepoResponse<JobModel> ingestFileLaunch(
        TestConfiguration.User user,
        String datasetId,
        String profileId,
        String sourceGsPath,
        String targetPath) throws Exception {

        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(sourceGsPath)
            .profileId(profileId)
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(targetPath);

        String json = objectMapper.writeValueAsString(fileLoadModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/files",
            json,
            JobModel.class);
    }

    public FileModel ingestFile(
        TestConfiguration.User user,
        String datasetId,
        String profileId,
        String sourceGsPath,
        String targetPath) throws Exception {
        DataRepoResponse<JobModel> resp = ingestFileLaunch(user, datasetId, profileId, sourceGsPath, targetPath);
        assertTrue("ingest launch succeeded", resp.getStatusCode().is2xxSuccessful());
        assertTrue("ingest launch response is present", resp.getResponseObject().isPresent());

        DataRepoResponse<FileModel> response = dataRepoClient.waitForResponse(user, resp, FileModel.class);
        assertThat("ingestFile is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestFile response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<FileModel> getFileByIdRaw(
        TestConfiguration.User user, String datasetId, String fileId) throws Exception {
        return dataRepoClient.get(
            user, "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId, FileModel.class);
    }

    public FileModel getFileById(TestConfiguration.User user, String datasetId, String fileId) throws Exception {
        DataRepoResponse<FileModel> response = getFileByIdRaw(user, datasetId, fileId);
        assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("file get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<FileModel> getFileByNameRaw(
        TestConfiguration.User user, String datasetId, String path) throws Exception {
        return dataRepoClient.get(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/filesystem/objects?path=" + path,
            FileModel.class);
    }

    public FileModel getFileByName(TestConfiguration.User user, String datasetId, String path) throws Exception {
        DataRepoResponse<FileModel> response = getFileByNameRaw(user, datasetId, path);
        assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("file get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<FileModel> getSnapshotFileByIdRaw(TestConfiguration.User user,
                                                                  String snapshotId,
                                                                  String fileId) throws Exception {
        return dataRepoClient.get(
            user, "/api/repository/v1/snapshots/" + snapshotId + "/files/" + fileId, FileModel.class);
    }

    public FileModel getSnapshotFileById(TestConfiguration.User user,
                                             String snapshotId,
                                             String fileId) throws Exception {
        DataRepoResponse<FileModel> response = getSnapshotFileByIdRaw(user, snapshotId, fileId);
        assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("file get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<FileModel> getSnapshotFileByNameRaw(TestConfiguration.User user,
                                                                    String snapshotId,
                                                                    String path) throws Exception {
        return dataRepoClient.get(
            user,
            "/api/repository/v1/snapshots/" + snapshotId + "/filesystem/objects?path=" + path,
            FileModel.class);
    }

    public FileModel getSnapshotFileByName(TestConfiguration.User user,
                                               String snapshotId,
                                               String path) throws Exception {
        DataRepoResponse<FileModel> response = getSnapshotFileByNameRaw(user, snapshotId, path);
        assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("file get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<JobModel> deleteFileLaunch(
        TestConfiguration.User user, String datasetId, String fileId) throws Exception {
        return dataRepoClient.delete(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId,
            JobModel.class);
    }

    public void deleteFile(
        TestConfiguration.User user, String datasetId, String fileId) throws Exception {
        DataRepoResponse<JobModel> launchResp = deleteFileLaunch(user, datasetId, fileId);
        assertTrue("delete launch succeeded", launchResp.getStatusCode().is2xxSuccessful());
        assertTrue("delete launch response is present", launchResp.getResponseObject().isPresent());
        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoClient.waitForResponse(
            user, launchResp, DeleteResponseModel.class);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DrsResponse<DRSObject> drsGetObjectRaw(TestConfiguration.User user, String drsObjectId) throws Exception {
        return dataRepoClient.drsGet(
            user,
            "/ga4gh/drs/v1/objects/" + drsObjectId,
            DRSObject.class);
    }

    public DRSObject drsGetObject(TestConfiguration.User user, String drsObjectId) throws Exception {
        DrsResponse<DRSObject> response = drsGetObjectRaw(user, drsObjectId);
        assertThat("object is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("object get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public Storage getStorage(String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        StorageOptions storageOptions = StorageOptions.newBuilder()
            .setCredentials(googleCredentials)
            .build();
        return storageOptions.getService();
    }

    public IngestRequestModel buildSimpleIngest(
        String table, String filename, IngestRequestModel.StrategyEnum strategy) throws Exception {
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + filename;
        return new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table(table)
            .path(gsPath)
            .strategy(strategy);
    }

    public DataRepoResponse<DeleteResponseModel> deleteProfile(
        TestConfiguration.User user,
        String profileId) throws Exception {
        return dataRepoClient.delete(
            user, "/api/resources/v1/profiles/" + profileId, DeleteResponseModel.class);
    }
}
