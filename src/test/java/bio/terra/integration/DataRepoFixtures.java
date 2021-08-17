package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import bio.terra.common.CloudPlatformWrapper;
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
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ConfigEnableModel;
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
import bio.terra.model.SnapshotRequestAccessIncludeModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.filedata.DrsResponse;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class DataRepoFixtures {

  private static Logger logger = LoggerFactory.getLogger(DataRepoFixtures.class);

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private TestConfiguration testConfig;

  // Create a Billing Profile model: expect successful creation
  public BillingProfileModel createBillingProfile(TestConfiguration.User user) throws Exception {
    BillingProfileRequestModel billingProfileRequestModel =
        ProfileFixtures.billingProfileRequest(
            ProfileFixtures.billingProfileForAccount(testConfig.getGoogleBillingAccountId()));
    String json = TestUtils.mapToJson(billingProfileRequestModel);

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.post(user, "/api/resources/v1/profiles", json, JobModel.class);
    assertTrue("profile create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<BillingProfileModel> postResponse =
        dataRepoClient.waitForResponse(user, jobResponse, BillingProfileModel.class);

    assertThat(
        "billing profile model is successfully created",
        postResponse.getStatusCode(),
        equalTo(HttpStatus.CREATED));
    assertTrue(
        "create billing profile model response is present",
        postResponse.getResponseObject().isPresent());
    return postResponse.getResponseObject().get();
  }

  // Create a Billing Profile model: expect successful creation
  public BillingProfileModel createAzureBillingProfile(TestConfiguration.User user)
      throws Exception {
    BillingProfileRequestModel billingProfileRequestModel =
        ProfileFixtures.billingProfileRequest(
            ProfileFixtures.billingProfileForDeployedApplication(
                testConfig.getTargetTenantId(),
                testConfig.getTargetSubscriptionId(),
                testConfig.getTargetResourceGroupName(),
                testConfig.getTargetApplicationName(),
                testConfig.getGoogleBillingAccountId()));
    String json = TestUtils.mapToJson(billingProfileRequestModel);

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.post(user, "/api/resources/v1/profiles", json, JobModel.class);
    assertTrue("profile create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<BillingProfileModel> postResponse =
        dataRepoClient.waitForResponse(user, jobResponse, BillingProfileModel.class);

    assertThat(
        "billing profile model is successfully created",
        postResponse.getStatusCode(),
        equalTo(HttpStatus.CREATED));
    assertTrue(
        "create billing profile model response is present",
        postResponse.getResponseObject().isPresent());
    return postResponse.getResponseObject().get();
  }

  public void deleteProfile(TestConfiguration.User user, UUID profileId) throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteProfileLog(user, profileId);
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<DeleteResponseModel> deleteProfileLog(
      TestConfiguration.User user, UUID profileId) throws Exception {

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.delete(user, "/api/resources/v1/profiles/" + profileId, JobModel.class);
    assertTrue("profile delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile delete launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, DeleteResponseModel.class);
  }

  // datasets

  private DataRepoResponse<JobModel> createDatasetRaw(
      TestConfiguration.User user, UUID profileId, String filename, CloudPlatform cloudPlatform)
      throws Exception {
    DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
    requestModel.setDefaultProfileId(profileId);
    requestModel.setName(Names.randomizeName(requestModel.getName()));
    if (cloudPlatform != null && requestModel.getCloudPlatform() == null) {
      requestModel.setCloudPlatform(cloudPlatform);
    }
    String json = TestUtils.mapToJson(requestModel);

    return dataRepoClient.post(user, "/api/repository/v1/datasets", json, JobModel.class);
  }

  public DatasetSummaryModel createDataset(
      TestConfiguration.User user, UUID profileId, String filename) throws Exception {
    return createDataset(user, profileId, filename, CloudPlatformWrapper.DEFAULT);
  }

  public DatasetSummaryModel createDataset(
      TestConfiguration.User user, UUID profileId, String filename, CloudPlatform cloudPlatform)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(user, profileId, filename, cloudPlatform);
    assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "dataset create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<DatasetSummaryModel> response =
        dataRepoClient.waitForResponseLog(user, jobResponse, DatasetSummaryModel.class);
    logger.info("Response was: {}", response);
    assertThat(
        "dataset create is successful", response.getStatusCode(), equalTo(HttpStatus.CREATED));
    assertTrue("dataset create response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public void createDatasetError(
      TestConfiguration.User user, UUID profileId, String filename, HttpStatus checkStatus)
      throws Exception {
    createDatasetError(user, profileId, filename, checkStatus, CloudPlatform.GCP);
  }

  public void createDatasetError(
      TestConfiguration.User user,
      UUID profileId,
      String filename,
      HttpStatus checkStatus,
      CloudPlatform cloudPlatform)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(user, profileId, filename, cloudPlatform);
    assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "dataset create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<ErrorModel> response =
        dataRepoClient.waitForResponse(user, jobResponse, ErrorModel.class);
    if (checkStatus == null) {
      assertFalse("dataset create is failure", response.getStatusCode().is2xxSuccessful());
    } else {
      assertThat("correct dataset create error", response.getStatusCode(), equalTo(checkStatus));
    }
    assertTrue("dataset create error response is present", response.getErrorObject().isPresent());
  }

  public DataRepoResponse<JobModel> deleteDataRaw(
      TestConfiguration.User user, UUID datasetId, DataDeletionRequest request) throws Exception {
    String url = String.format("/api/repository/v1/datasets/%s/deletes", datasetId);
    String json = TestUtils.mapToJson(request);
    return dataRepoClient.post(user, url, json, JobModel.class);
  }

  public void deleteData(TestConfiguration.User user, UUID datasetId, DataDeletionRequest request)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse = deleteDataRaw(user, datasetId, request);
    DataRepoResponse<DeleteResponseModel> deleteResponse =
        dataRepoClient.waitForResponse(user, jobResponse, DeleteResponseModel.class);
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<JobModel> deleteDatasetLaunch(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    return dataRepoClient.delete(user, "/api/repository/v1/datasets/" + datasetId, JobModel.class);
  }

  public void deleteDataset(TestConfiguration.User user, UUID datasetId) throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetLog(user, datasetId);
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<DeleteResponseModel> deleteDatasetLog(
      TestConfiguration.User user, UUID datasetId) throws Exception {

    DataRepoResponse<JobModel> jobResponse = deleteDatasetLaunch(user, datasetId);
    assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, DeleteResponseModel.class);
  }

  public DataRepoResponse<EnumerateDatasetModel> enumerateDatasetsRaw(TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/datasets?sort=created_date&direction=desc",
        EnumerateDatasetModel.class);
  }

  public EnumerateDatasetModel enumerateDatasets(TestConfiguration.User user) throws Exception {
    DataRepoResponse<EnumerateDatasetModel> response = enumerateDatasetsRaw(user);
    assertThat(
        "dataset enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("dataset get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<DatasetModel> getDatasetRaw(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    return dataRepoClient.get(user, "/api/repository/v1/datasets/" + datasetId, DatasetModel.class);
  }

  public DatasetModel getDataset(TestConfiguration.User user, UUID datasetId) throws Exception {
    DataRepoResponse<DatasetModel> response = getDatasetRaw(user, datasetId);
    assertThat(
        "dataset is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("dataset get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<Object> addPolicyMemberRaw(
      TestConfiguration.User user,
      UUID resourceId,
      IamRole role,
      String userEmail,
      IamResourceType iamResourceType)
      throws Exception {
    PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
    String pathPrefix;
    switch (iamResourceType) {
      case DATASET:
        pathPrefix = "/api/repository/v1/datasets/";
        break;
      case DATASNAPSHOT:
        pathPrefix = "/api/repository/v1/snapshots/";
        break;
      case SPEND_PROFILE:
        pathPrefix = "/api/resources/v1/profiles/";
        break;
      default:
        throw new IllegalArgumentException(
            "No path prefix defined for IamResourceType " + iamResourceType);
    }
    String path = pathPrefix + resourceId + "/policies/" + role.toString() + "/members";

    return dataRepoClient.post(user, path, TestUtils.mapToJson(req), null);
  }

  public void addPolicyMember(
      TestConfiguration.User user,
      UUID resourceId,
      IamRole role,
      String newMemberEmail,
      IamResourceType iamResourceType)
      throws Exception {
    DataRepoResponse<Object> response =
        addPolicyMemberRaw(user, resourceId, role, newMemberEmail, iamResourceType);
    assertThat(
        iamResourceType + " policy member is successfully added",
        response.getStatusCode(),
        equalTo(HttpStatus.OK));
  }

  // adding dataset policy
  public void addDatasetPolicyMember(
      TestConfiguration.User user, UUID datasetId, IamRole role, String newMemberEmail)
      throws Exception {
    addPolicyMember(user, datasetId, role, newMemberEmail, IamResourceType.DATASET);
  }

  // adding dataset asset
  public DataRepoResponse<JobModel> addDatasetAssetRaw(
      TestConfiguration.User user, UUID datasetId, AssetModel assetModel) throws Exception {
    return dataRepoClient.post(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/assets",
        TestUtils.mapToJson(assetModel),
        JobModel.class);
  }

  public void addDatasetAsset(TestConfiguration.User user, UUID datasetId, AssetModel assetModel)
      throws Exception {
    // TODO add the assetModel as a builder object
    DataRepoResponse<JobModel> response = addDatasetAssetRaw(user, datasetId, assetModel);
    assertTrue(
        assetModel + " asset specification is successfully added",
        response.getStatusCode().is2xxSuccessful());
  }

  // snapshots

  // adding snapshot policy
  public void addSnapshotPolicyMember(
      TestConfiguration.User user, UUID snapshotId, IamRole role, String newMemberEmail)
      throws Exception {
    addPolicyMember(user, snapshotId, role, newMemberEmail, IamResourceType.DATASNAPSHOT);
  }

  public DataRepoResponse<JobModel> createSnapshotRaw(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel requestModel,
      boolean randomizeName)
      throws Exception {

    if (randomizeName) {
      requestModel.setName(Names.randomizeName(requestModel.getName()));
    }
    requestModel.getContents().get(0).setDatasetName(datasetName);
    requestModel.setProfileId(profileId);
    String json = TestUtils.mapToJson(requestModel);

    return dataRepoClient.post(user, "/api/repository/v1/snapshots", json, JobModel.class);
  }

  public SnapshotSummaryModel createSnapshotWithRequest(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel snapshotRequest)
      throws Exception {
    return createSnapshotWithRequest(user, datasetName, profileId, snapshotRequest, true);
  }

  public SnapshotSummaryModel createSnapshotWithRequest(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel snapshotRequest,
      boolean randomizeName)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createSnapshotRaw(user, datasetName, profileId, snapshotRequest, randomizeName);
    return finishCreateSnapshot(user, jobResponse);
  }

  public SnapshotSummaryModel createSnapshot(
      TestConfiguration.User user, String datasetName, UUID profileId, String filename)
      throws Exception {
    return createSnapshot(user, datasetName, profileId, filename, true);
  }

  public SnapshotSummaryModel createSnapshot(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      String filename,
      boolean randomizeName)
      throws Exception {
    SnapshotRequestModel requestModel = jsonLoader.loadObject(filename, SnapshotRequestModel.class);
    DataRepoResponse<JobModel> jobResponse =
        createSnapshotRaw(user, datasetName, profileId, requestModel, randomizeName);
    return finishCreateSnapshot(user, jobResponse);
  }

  private SnapshotSummaryModel finishCreateSnapshot(
      TestConfiguration.User user, DataRepoResponse<JobModel> jobResponse) throws Exception {
    assertTrue("snapshot create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "snapshot create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<SnapshotSummaryModel> snapshotResponse =
        dataRepoClient.waitForResponse(user, jobResponse, SnapshotSummaryModel.class);
    assertThat(
        "snapshot create is successful",
        snapshotResponse.getStatusCode(),
        equalTo(HttpStatus.CREATED));
    assertTrue(
        "snapshot create response is present", snapshotResponse.getResponseObject().isPresent());
    return snapshotResponse.getResponseObject().get();
  }

  public DataRepoResponse<SnapshotModel> getSnapshotRaw(
      TestConfiguration.User user, UUID snapshotId, List<SnapshotRequestAccessIncludeModel> include)
      throws Exception {
    String includeParam;
    if (include != null && !include.isEmpty()) {
      includeParam =
          "?"
              + include.stream()
                  .map(i -> "include=" + i.toString())
                  .collect(Collectors.joining("&"));
    } else {
      includeParam = "";
    }
    return dataRepoClient.get(
        user, "/api/repository/v1/snapshots/" + snapshotId + includeParam, SnapshotModel.class);
  }

  public SnapshotModel getSnapshot(
      TestConfiguration.User user, UUID snapshotId, List<SnapshotRequestAccessIncludeModel> include)
      throws Exception {
    DataRepoResponse<SnapshotModel> response = getSnapshotRaw(user, snapshotId, include);
    assertThat(
        "dataset is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("dataset get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<EnumerateSnapshotModel> enumerateSnapshotsByDatasetIdsRaw(
      TestConfiguration.User user, List<UUID> datasetIds) throws Exception {
    String datasetIdsString;
    List<String> datasetIdsQuery =
        ListUtils.emptyIfNull(datasetIds).stream()
            .map(id -> "datasetIds=" + id)
            .collect(Collectors.toList());
    datasetIdsString = StringUtils.join(datasetIdsQuery, "&");
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots?" + datasetIdsString + "&sort=created_date&direction=desc",
        EnumerateSnapshotModel.class);
  }

  public EnumerateSnapshotModel enumerateSnapshotsByDatasetIds(
      TestConfiguration.User user, List<UUID> datasetIds) throws Exception {
    DataRepoResponse<EnumerateSnapshotModel> response =
        enumerateSnapshotsByDatasetIdsRaw(user, datasetIds);
    assertThat(
        "snapshot enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("snapshot get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<EnumerateSnapshotModel> enumerateSnapshotsRaw(TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots?sort=created_date&direction=desc",
        EnumerateSnapshotModel.class);
  }

  public EnumerateSnapshotModel enumerateSnapshots(TestConfiguration.User user) throws Exception {
    DataRepoResponse<EnumerateSnapshotModel> response = enumerateSnapshotsRaw(user);
    assertThat(
        "snapshot enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("snapshot get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public void deleteSnapshot(TestConfiguration.User user, UUID snapshotId) throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteSnapshotLog(user, snapshotId);
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<DeleteResponseModel> deleteSnapshotLog(
      TestConfiguration.User user, UUID snapshotId) throws Exception {

    DataRepoResponse<JobModel> jobResponse = deleteSnapshotLaunch(user, snapshotId);
    assertTrue("snapshot delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "snapshot delete launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, DeleteResponseModel.class);
  }

  public DataRepoResponse<JobModel> deleteSnapshotLaunch(
      TestConfiguration.User user, UUID snapshotId) throws Exception {
    return dataRepoClient.delete(
        user, "/api/repository/v1/snapshots/" + snapshotId, JobModel.class);
  }

  private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {

    assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
    DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
    assertTrue(
        "Valid delete response",
        (deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED
            || deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
  }

  public DataRepoResponse<JobModel> ingestJsonDataLaunch(
      TestConfiguration.User user, UUID datasetId, IngestRequestModel request) throws Exception {
    String ingestBody = TestUtils.mapToJson(request);
    return dataRepoClient.post(
        user, "/api/repository/v1/datasets/" + datasetId + "/ingest", ingestBody, JobModel.class);
  }

  public IngestResponseModel ingestJsonData(
      TestConfiguration.User user, UUID datasetId, IngestRequestModel request) throws Exception {

    DataRepoResponse<JobModel> launchResp = ingestJsonDataLaunch(user, datasetId, request);
    assertTrue("ingest launch succeeded", launchResp.getStatusCode().is2xxSuccessful());
    assertTrue("ingest launch response is present", launchResp.getResponseObject().isPresent());
    DataRepoResponse<IngestResponseModel> response =
        dataRepoClient.waitForResponse(user, launchResp, IngestResponseModel.class);

    assertThat("ingestOne is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("ingestOne response is present", response.getResponseObject().isPresent());

    IngestResponseModel ingestResponse = response.getResponseObject().get();
    assertThat("no bad sample rows", ingestResponse.getBadRowCount(), equalTo(0L));
    return ingestResponse;
  }

  public DataRepoResponse<JobModel> ingestFileLaunch(
      TestConfiguration.User user,
      UUID datasetId,
      UUID profileId,
      String sourceGsPath,
      String targetPath)
      throws Exception {

    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceGsPath)
            .profileId(profileId)
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(targetPath);

    String json = TestUtils.mapToJson(fileLoadModel);

    return dataRepoClient.post(
        user, "/api/repository/v1/datasets/" + datasetId + "/files", json, JobModel.class);
  }

  public FileModel ingestFile(
      TestConfiguration.User user,
      UUID datasetId,
      UUID profileId,
      String sourceGsPath,
      String targetPath)
      throws Exception {
    DataRepoResponse<JobModel> resp =
        ingestFileLaunch(user, datasetId, profileId, sourceGsPath, targetPath);
    assertTrue("ingest launch succeeded", resp.getStatusCode().is2xxSuccessful());
    assertTrue("ingest launch response is present", resp.getResponseObject().isPresent());

    DataRepoResponse<FileModel> response =
        dataRepoClient.waitForResponse(user, resp, FileModel.class);
    assertThat("ingestFile is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("ingestFile response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public BulkLoadArrayResultModel bulkLoadArray(
      TestConfiguration.User user, UUID datasetId, BulkLoadArrayRequestModel requestModel)
      throws Exception {

    String json = TestUtils.mapToJson(requestModel);

    DataRepoResponse<JobModel> launchResponse =
        dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/files/bulk/array",
            json,
            JobModel.class);
    assertTrue("bulkLoadArray launch succeeded", launchResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "bulkloadArray launch response is present", launchResponse.getResponseObject().isPresent());

    DataRepoResponse<BulkLoadArrayResultModel> response =
        dataRepoClient.waitForResponse(user, launchResponse, BulkLoadArrayResultModel.class);
    if (response.getStatusCode().is2xxSuccessful()) {
      assertThat("bulkLoadArray is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
      assertTrue("ingestFile response is present", response.getResponseObject().isPresent());
      return response.getResponseObject().get();
    }
    ErrorModel errorModel = response.getErrorObject().orElse(null);
    logger.error("bulkLoadArray failed: " + errorModel);
    fail();
    return null; // Make findbugs happy
  }

  public BulkLoadHistoryModelList getLoadHistory(
      TestConfiguration.User user, UUID datasetId, String loadTag, int offset, int limit)
      throws Exception {
    var response =
        dataRepoClient.get(
            user,
            "/api/repository/v1/datasets/"
                + datasetId
                + "/files/bulk/"
                + loadTag
                + "?offset="
                + offset
                + "&limit="
                + limit,
            BulkLoadHistoryModelList.class);
    if (response.getStatusCode().is2xxSuccessful()) {
      assertThat("getLoadHistory is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
      assertTrue("getLoadHistory response is present", response.getResponseObject().isPresent());
      return response.getResponseObject().get();
    }
    ErrorModel errorModel = response.getErrorObject().orElse(null);
    logger.error("getLoadHistory failed: " + errorModel);
    fail();
    return null; // Make findbugs happy
  }

  public DataRepoResponse<FileModel> getFileByIdRaw(
      TestConfiguration.User user, UUID datasetId, String fileId) throws Exception {
    return dataRepoClient.get(
        user, "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId, FileModel.class);
  }

  public FileModel getFileById(TestConfiguration.User user, UUID datasetId, String fileId)
      throws Exception {
    DataRepoResponse<FileModel> response = getFileByIdRaw(user, datasetId, fileId);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<FileModel> getFileByNameRaw(
      TestConfiguration.User user, UUID datasetId, String path) throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/filesystem/objects?path=" + path,
        FileModel.class);
  }

  public FileModel getFileByName(TestConfiguration.User user, UUID datasetId, String path)
      throws Exception {
    DataRepoResponse<FileModel> response = getFileByNameRaw(user, datasetId, path);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<FileModel> getSnapshotFileByIdRaw(
      TestConfiguration.User user, UUID snapshotId, String fileId) throws Exception {
    return dataRepoClient.get(
        user, "/api/repository/v1/snapshots/" + snapshotId + "/files/" + fileId, FileModel.class);
  }

  public FileModel getSnapshotFileById(TestConfiguration.User user, UUID snapshotId, String fileId)
      throws Exception {
    DataRepoResponse<FileModel> response = getSnapshotFileByIdRaw(user, snapshotId, fileId);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<FileModel> getSnapshotFileByNameRaw(
      TestConfiguration.User user, UUID snapshotId, String path) throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots/" + snapshotId + "/filesystem/objects?path=" + path,
        FileModel.class);
  }

  public FileModel getSnapshotFileByName(TestConfiguration.User user, UUID snapshotId, String path)
      throws Exception {
    DataRepoResponse<FileModel> response = getSnapshotFileByNameRaw(user, snapshotId, path);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<JobModel> deleteFileLaunch(
      TestConfiguration.User user, UUID datasetId, String fileId) throws Exception {
    return dataRepoClient.delete(
        user, "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId, JobModel.class);
  }

  public void deleteFile(TestConfiguration.User user, UUID datasetId, String fileId)
      throws Exception {
    DataRepoResponse<JobModel> launchResp = deleteFileLaunch(user, datasetId, fileId);
    assertTrue("delete launch succeeded", launchResp.getStatusCode().is2xxSuccessful());
    assertTrue("delete launch response is present", launchResp.getResponseObject().isPresent());
    DataRepoResponse<DeleteResponseModel> deleteResponse =
        dataRepoClient.waitForResponse(user, launchResp, DeleteResponseModel.class);
    assertGoodDeleteResponse(deleteResponse);
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  public DrsResponse<DRSObject> drsGetObjectRaw(TestConfiguration.User user, String drsObjectId)
      throws Exception {
    return dataRepoClient.drsGet(user, "/ga4gh/drs/v1/objects/" + drsObjectId, DRSObject.class);
  }

  public DrsResponse<bio.terra.model.DRSAccessURL> getObjectAccessUrl(
      TestConfiguration.User user, String drsObjectId, String accessId) throws Exception {
    return dataRepoClient.drsGet(
        user,
        "/ga4gh/drs/v1/objects/" + drsObjectId + "/access/" + accessId,
        bio.terra.model.DRSAccessURL.class);
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  public DRSObject drsGetObject(TestConfiguration.User user, String drsObjectId) throws Exception {
    DrsResponse<DRSObject> response = drsGetObjectRaw(user, drsObjectId);
    assertThat(
        "object is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("object get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public Storage getStorage(String token) {
    GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
    StorageOptions storageOptions =
        StorageOptions.newBuilder().setCredentials(googleCredentials).build();
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

  // Configuration methods

  public DataRepoResponse<ConfigModel> getConfig(TestConfiguration.User user, String configName)
      throws Exception {
    return dataRepoClient.get(user, "/api/repository/v1/configs/" + configName, ConfigModel.class);
  }

  public DataRepoResponse<ConfigModel> setFault(
      TestConfiguration.User user, String configName, boolean enable) throws Exception {
    ConfigEnableModel configEnableModel = new ConfigEnableModel().enabled(enable);
    String json = TestUtils.mapToJson(configEnableModel);
    return dataRepoClient.put(
        user,
        "/api/repository/v1/configs/" + configName,
        json,
        null); // TODO should this validation on returned value?
  }

  public DataRepoResponse<Void> resetConfig(TestConfiguration.User user) throws Exception {
    return dataRepoClient.put(user, "/api/repository/v1/configs/reset", null, null);
  }

  public DataRepoResponse<ConfigListModel> setConfigListRaw(
      TestConfiguration.User user, ConfigGroupModel configGroup) throws Exception {
    String json = TestUtils.mapToJson(configGroup);
    return dataRepoClient.put(user, "/api/repository/v1/configs", json, ConfigListModel.class);
  }

  public ConfigListModel setConfigList(TestConfiguration.User user, ConfigGroupModel configGroup)
      throws Exception {
    DataRepoResponse<ConfigListModel> response = setConfigListRaw(user, configGroup);
    assertThat("setConfigList is successfully", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("setConfigList response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<ConfigListModel> getConfigListRaw(TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(user, "/api/repository/v1/configs", ConfigListModel.class);
  }

  public ConfigListModel getConfigList(TestConfiguration.User user) throws Exception {
    DataRepoResponse<ConfigListModel> response = getConfigListRaw(user);
    assertThat("getConfigList is successfully", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("getConfigList response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }
}
