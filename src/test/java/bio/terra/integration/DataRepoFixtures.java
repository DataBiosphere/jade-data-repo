package bio.terra.integration;

import bio.terra.common.TestUtils;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigListModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.filedata.DrsResponse;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Component
public class DataRepoFixtures {
    private static Logger logger = LoggerFactory.getLogger(DataRepoFixtures.class);

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    // Create a Billing Profile model: expect successful creation
    public BillingProfileModel createBillingProfile(TestConfiguration.User user) throws Exception {
        BillingProfileRequestModel billingProfileRequestModel = ProfileFixtures.randomBillingProfileRequest();
        String json = TestUtils.mapToJson(billingProfileRequestModel);
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

    public DataRepoResponse<JobModel> createDatasetRaw(TestConfiguration.User user, String filename)
        throws Exception {
        DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
        BillingProfileModel billingProfileModel = this.createBillingProfile(user);
        requestModel.setDefaultProfileId(billingProfileModel.getId());
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = TestUtils.mapToJson(requestModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/datasets",
            json,
            JobModel.class);
    }

    public DatasetSummaryModel createDataset(TestConfiguration.User user, String filename) throws Exception {
        DataRepoResponse<JobModel> jobResponse = createDatasetRaw(user, filename);
        assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset create launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DatasetSummaryModel> response = dataRepoClient.waitForResponse(
            user, jobResponse, DatasetSummaryModel.class);
        assertThat("dataset create is successful", response.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public ErrorModel createDatasetError(TestConfiguration.User user,
                                         String filename,
                                         HttpStatus checkStatus) throws Exception {
        DataRepoResponse<JobModel> jobResponse = createDatasetRaw(user, filename);
        assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset create launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<ErrorModel> response = dataRepoClient.waitForResponse(user, jobResponse, ErrorModel.class);
        if (checkStatus == null) {
            assertFalse("dataset create is failure", response.getStatusCode().is2xxSuccessful());
        } else {
            assertThat("correct dataset create error", response.getStatusCode(), equalTo(checkStatus));
        }
        assertTrue("dataset create error response is present", response.getErrorObject().isPresent());
        return response.getErrorObject().get();
    }

    public DataRepoResponse<JobModel> deleteDataRaw(TestConfiguration.User user,
                                                    String datasetId,
                                                    DataDeletionRequest request) throws Exception {
        String url = String.format("/api/repository/v1/datasets/%s/deletes", datasetId);
        String json = TestUtils.mapToJson(request);
        return dataRepoClient.post(user, url, json, JobModel.class);
    }

    public void deleteData(TestConfiguration.User user,
                           String datasetId,
                           DataDeletionRequest request) throws Exception {
        DataRepoResponse<JobModel> jobResponse = deleteDataRaw(user, datasetId, request);
        DataRepoResponse<DeleteResponseModel> deleteResponse =
            dataRepoClient.waitForResponse(user, jobResponse, DeleteResponseModel.class);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<JobModel> deleteDatasetLaunch(TestConfiguration.User user, String datasetId)
        throws Exception {
        return dataRepoClient.delete(
            user, "/api/repository/v1/datasets/" + datasetId, JobModel.class);
    }

    public void deleteDataset(TestConfiguration.User user, String datasetId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetLog(user, datasetId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<DeleteResponseModel> deleteDatasetLog(TestConfiguration.User user, String datasetId)
        throws Exception {

        DataRepoResponse<JobModel> jobResponse = deleteDatasetLaunch(user, datasetId);
        assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoClient.waitForResponse(
            user, jobResponse, DeleteResponseModel.class);
        // if not successful, log the response
        if (!deleteResponse.getStatusCode().is2xxSuccessful()) {
            logger.error("delete operation failed");
            if (deleteResponse.getErrorObject().isPresent()) {
                logger.error("error object: " + deleteResponse.getErrorObject().get());
            }
        }
        return deleteResponse;
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
                                                       IamRole role,
                                                       String userEmail,
                                                       IamResourceType iamResourceType) throws Exception {
        PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
        return dataRepoClient.post(user, "/api/repository/v1/" + TestUtils.getHttpPathString(iamResourceType) +
                "/" + resourceId + "/policies/" + role.toString() + "/members",
            TestUtils.mapToJson(req), null);
    }

    public void addPolicyMember(TestConfiguration.User user,
                                String resourceId,
                                IamRole role,
                                String newMemberEmail,
                                IamResourceType iamResourceType) throws Exception {
        DataRepoResponse<Object> response =
            addPolicyMemberRaw(user, resourceId, role, newMemberEmail, iamResourceType);
        assertThat(iamResourceType + " policy member is successfully added",
            response.getStatusCode(), equalTo(HttpStatus.OK));
    }


    // adding dataset policy
    public void addDatasetPolicyMember(TestConfiguration.User user,
                                                   String datasetId,
                                                   IamRole role,
                                                   String newMemberEmail) throws Exception {
        addPolicyMember(user, datasetId, role, newMemberEmail, IamResourceType.DATASET);
    }

    // adding dataset asset
    public DataRepoResponse<JobModel> addDatasetAssetRaw(TestConfiguration.User user,
                                   String datasetId,
                                   AssetModel assetModel) throws Exception {
        return dataRepoClient.post(user, "/api/repository/v1/datasets/" + datasetId + "/assets",
            TestUtils.mapToJson(assetModel), JobModel.class);
    }

    public void addDatasetAsset(TestConfiguration.User user,
                                String datasetId,
                                AssetModel assetModel) throws Exception {
        // TODO add the assetModel as a builder object
        DataRepoResponse<JobModel> response = addDatasetAssetRaw(user, datasetId, assetModel);
        assertTrue(assetModel + " asset specification is successfully added",
            response.getStatusCode().is2xxSuccessful());
    }

    // snapshots

    // adding snapshot policy
    public void addSnapshotPolicyMember(TestConfiguration.User user,
                                       String snapshotId,
                                       IamRole role,
                                       String newMemberEmail) throws Exception {
        addPolicyMember(user, snapshotId, role, newMemberEmail, IamResourceType.DATASNAPSHOT);
    }

    public DataRepoResponse<JobModel> createSnapshotWithRequestLaunch(
        TestConfiguration.User user,
        String datasetName,
        SnapshotRequestModel requestModel) throws Exception {
        BillingProfileModel billingProfileModel = this.createBillingProfile(user);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        requestModel.getContents().get(0).setDatasetName(datasetName);
        requestModel.setProfileId(billingProfileModel.getId());
        String json = TestUtils.mapToJson(requestModel);

        return dataRepoClient.post(
            user,
            "/api/repository/v1/snapshots",
            json,
            JobModel.class);
    }

    public DataRepoResponse<JobModel> createSnapshotLaunch(
        TestConfiguration.User user, DatasetSummaryModel datasetSummaryModel, String filename) throws Exception {
        SnapshotRequestModel requestModel = jsonLoader.loadObject(filename, SnapshotRequestModel.class);
        return createSnapshotWithRequestLaunch(user, datasetSummaryModel.getName(), requestModel);
    }

    public SnapshotSummaryModel resolveCreateSnapshot(
        TestConfiguration.User user,
        DataRepoResponse<JobModel> jobResponse) throws Exception {
        assertTrue("snapshot create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("snapshot create launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<SnapshotSummaryModel> snapshotResponse = dataRepoClient.waitForResponse(
            user, jobResponse, SnapshotSummaryModel.class);
        assertThat("snapshot create is successful", snapshotResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("snapshot create response is present", snapshotResponse.getResponseObject().isPresent());
        return snapshotResponse.getResponseObject().get();
    }

    public SnapshotSummaryModel createSnapshotWithRequest(
        TestConfiguration.User user,
        String datasetName,
        SnapshotRequestModel snapshotRequest) throws Exception {
        DataRepoResponse<JobModel> jobResponse =
            createSnapshotWithRequestLaunch(user, datasetName, snapshotRequest);
        return resolveCreateSnapshot(user, jobResponse);
    }

    public SnapshotSummaryModel createSnapshot(
        TestConfiguration.User user, DatasetSummaryModel datasetSummaryModel, String filename) throws Exception {
        DataRepoResponse<JobModel> jobResponse = createSnapshotLaunch(
            user, datasetSummaryModel, filename);
        return resolveCreateSnapshot(user, jobResponse);
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
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteSnapshotLog(user, snapshotId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<DeleteResponseModel> deleteSnapshotLog(TestConfiguration.User user, String snapshotId)
        throws Exception {

        DataRepoResponse<JobModel> jobResponse = deleteSnapshotLaunch(user, snapshotId);
        assertTrue("snapshot delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("snapshot delete launch response is present", jobResponse.getResponseObject().isPresent());

        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoClient.waitForResponse(
            user, jobResponse, DeleteResponseModel.class);
        // if not successful, log the response
        if (!deleteResponse.getStatusCode().is2xxSuccessful()) {
            logger.error("delete operation failed");
            if (deleteResponse.getErrorObject().isPresent()) {
                logger.error("error object: " + deleteResponse.getErrorObject().get());
            }
        }
        return deleteResponse;
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
        String ingestBody = TestUtils.mapToJson(request);
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

        String json = TestUtils.mapToJson(fileLoadModel);

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

    public BulkLoadArrayResultModel bulkLoadArray(
        TestConfiguration.User user,
        String datasetId,
        BulkLoadArrayRequestModel requestModel) throws Exception {

        String json = TestUtils.mapToJson(requestModel);

        DataRepoResponse<JobModel> launchResponse = dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/files/bulk/array",
            json,
            JobModel.class);
        assertTrue("bulkLoadArray launch succeeded", launchResponse.getStatusCode().is2xxSuccessful());
        assertTrue("bulkloadArray launch response is present", launchResponse.getResponseObject().isPresent());

        DataRepoResponse<BulkLoadArrayResultModel> response =
            dataRepoClient.waitForResponse(user, launchResponse, BulkLoadArrayResultModel.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            assertThat("bulkLoadArray is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
            assertTrue("ingestFile response is present", response.getResponseObject().isPresent());
            return response.getResponseObject().get();
        } else {
            ErrorModel errorModel = response.getErrorObject().orElse(null);
            logger.error("bulkLoadArray failed: " + errorModel);
            fail();
            return null; // Make findbugs happy
        }
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

    public IngestRequestModel buildSimpleIngest(String table, String filename) throws Exception {
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + filename;
        return new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(table)
            .path(gsPath);
    }

    public DataRepoResponse<DeleteResponseModel> deleteProfile(
        TestConfiguration.User user,
        String profileId) throws Exception {
        return dataRepoClient.delete(
            user, "/api/resources/v1/profiles/" + profileId, DeleteResponseModel.class);
    }

    // Configuration methods

    public DataRepoResponse<ConfigModel> getConfig(TestConfiguration.User user, String configName) throws Exception {
        return dataRepoClient.get(
            user, "/api/repository/v1/configs/" + configName, ConfigModel.class);
    }

    public DataRepoResponse<ConfigModel> setFault(TestConfiguration.User user,
                                                  String configName,
                                                  boolean enable) throws Exception {
        return dataRepoClient.put(user,
            "/api/repository/v1/configs/" + configName + "?enable=" + enable,
            null,
            null); // TODO should this validation on returned value?
    }

    public DataRepoResponse<Void> resetConfig(TestConfiguration.User user) throws Exception {
        return dataRepoClient.put(user,
            "/api/repository/v1/configs/reset",
            null,
            null);
    }

    public DataRepoResponse<ConfigListModel> setConfigListRaw(TestConfiguration.User user,
                                                              ConfigGroupModel configGroup) throws Exception {
        String json = TestUtils.mapToJson(configGroup);
        return dataRepoClient.put(user,
            "/api/repository/v1/configs",
            json,
            ConfigListModel.class);
    }

    public ConfigListModel setConfigList(TestConfiguration.User user,
                                           ConfigGroupModel configGroup) throws Exception {
        DataRepoResponse<ConfigListModel> response = setConfigListRaw(user, configGroup);
        assertThat("setConfigList is successfully", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("setConfigList response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<ConfigListModel> getConfigListRaw(TestConfiguration.User user) throws Exception {
        return dataRepoClient.get(user, "/api/repository/v1/configs", ConfigListModel.class);
    }

    public ConfigListModel getConfigList(TestConfiguration.User user) throws Exception {
        DataRepoResponse<ConfigListModel> response = getConfigListRaw(user);
        assertThat("getConfigList is successfully", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("getConfigList response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

}
